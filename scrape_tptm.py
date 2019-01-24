"""Scrape Talk python to me.

URL: https://talkpython.fm/

# Refactor:
    - Make to class
    - Add config
    - Improve logging

# TODO:
    - Add docstrings
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from pyvirtualdisplay import Display
import time
import pickle
import datetime
import pytz
import re
import numpy as np

from loguru import logger

import os

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")


TPTM_BASE_URL = "https://talkpython.fm"
TPTM_EPISODES_URL = "https://talkpython.fm/episodes/all"

display = Display(visible=0, size=(800, 600))
display.start()


def get_driver():

    # options = webdriver.ChromeOptions()
    # options.add_argument("headless")
    # chromedriver = "/Applications/chromedriver"  # path to the chromedriver executable
    # os.environ["webdriver.chrome.driver"] = chromedriver
    # return webdriver.Chrome(chromedriver, chrome_options=options)
    return webdriver.Firefox()


def save_to_pickle(data, filename):
    with open(filename, "wb") as f:
        pickle.dump(data, f)


def get_from_pickle(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


@logger.catch
def try_to_load_from_pickle(load_from_pickle=True, filename=None, **kwargs):
    """If there is a pickle file with the filename, return its content,
    otherwise scrape the data.

        load_from_pickle: Ignore pickled file.
    """

    if load_from_pickle and filename:
        try:
            data = get_from_pickle(filename)
            logger.info(f"Success! Loaded data from {filename}.")

            return data
        except FileNotFoundError:
            logger.info(
                f"Could not find file: {filename}. Can not load from pickle! Continue ..."
            )
            pass

    return None


@logger.catch
def try_to_save_to_pickle(data, dump_to_pickle=True, filename=None, **kwargs):
    """Try to save data to a pickle file.

    Note: There should not be a reason why this does not work.
    """

    if dump_to_pickle and filename:
        try:
            save_to_pickle(data, filename)
            logger.info(f"Success! Stored data in {filename}.")
        except FileNotFoundError:
            logger.info(
                f"Could not find file: {filename}. Can not save to pickle! Continue ..."
            )
            pass


@logger.catch
def get_episode_list(**kwargs):
    """Scrape the episode list from the TPTM_EPISODES_URL.
    """
    logger.info(f"Get Episode List for {kwargs}")
    data = try_to_load_from_pickle(**kwargs)
    if data:
        logger.info(f"Got data from pickle.")
        return data, True

    driver = get_driver()
    driver.get(kwargs["url"])

    try:
        episodes_table = driver.find_elements_by_class_name("episodes")
        episodes_table_body = episodes_table[1].find_element_by_tag_name("tbody")
        episode_rows = episodes_table_body.find_elements_by_tag_name("tr")
    except NoSuchElementException as e:
        logger.error(e)
        return None, False

    entries_list = []
    for idx, row in enumerate(episode_rows):
        entries = row.find_elements_by_tag_name("td")

        episode_dict = {
            "idx": idx,
            "show_number": entries[0].text,
            "date": entries[1].text,
            "title": entries[2].text,
            "episode_url": entries[2]
            .find_element_by_tag_name("a")
            .get_property("href"),
            "guests": entries[3].text,
        }
        logger.info(
            f"Scraped Show {episode_dict['show_number']}, with idx: {idx}, named: {episode_dict['title']} "
        )
        entries_list.append(episode_dict)

    entries = remove_none_from_list(entries_list)

    try_to_save_to_pickle(entries_list, **kwargs)

    driver.close()
    logger.success(f"Got Episode List.")
    return entries_list, False


@logger.catch
def get_mentioned_links_for_episode(episode, **kwargs):

    show_number = episode.get("show_number", "999999")
    show_number = show_number.replace("#", "")
    show_number = int(show_number)

    data = try_to_load_from_pickle(
        filename=f"data/episodes/cleaned_episode_{show_number}.pk"
    )
    if data is not None:
        return data, True

    # TODO: Refactor to ContextManager
    time.sleep(get_random_wait_time())
    driver = get_driver()
    driver.get(episode["episode_url"])
    try:
        episode_dates_info = driver.find_element_by_class_name("published-date").text
        episode_description = driver.find_element_by_class_name("large-content-text")
        episode_references = episode_description.find_element_by_tag_name(
            "div"
        ).find_elements_by_tag_name("a")
    except NoSuchElementException as e:
        logger.error(e)
        logger.error(f"Encounterd a problem when retrieving info for episode {episode}")
        logger.error(f"Ignoring this episode")
        return None, False

    reference_list = []
    for reference in episode_references:
        reference_list.append(
            {"text": reference.text, "url": reference.get_property("href")}
        )

    episode["dates_info"] = episode_dates_info
    episode["reference_list"] = remove_none_from_list(reference_list)
    logger.info(
        f"Got info for episode {episode['show_number']} with idx: {episode['idx']}, named: {episode['title']}"
    )

    driver.close()
    return episode, False


@logger.catch
def clean_episode_dates_info(episode, **kwargs):
    date_format = "%Y-%m-%d"
    date_published = convert_to_datetime(episode["date"], date_format)
    dates_info = episode["dates_info"]

    dates_info_re = r"Published\s\w+,\s(?P<date_published>\w+\s\d+,\s\d+),\srecorded\s\w+,\s(?P<date_recorded>\w+\s\d+,\s\d+)."
    dates_info_match = re.search(dates_info_re, dates_info)
    date_published_2 = dates_info_match.group("date_published")
    date_recorded = dates_info_match.group("date_recorded")
    dates_info_format = "%b %d, %Y"

    date_published_2 = convert_to_datetime(date_published_2, dates_info_format)
    date_recorded = convert_to_datetime(date_recorded, dates_info_format)

    if date_published != date_published_2:
        episode["date_published_2"] = date_published_2

    episode["date_published"] = date_published
    episode["date_recorded"] = date_recorded

    episode.pop("dates_info", None)
    episode.pop("date", None)

    return episode


def clean_episode_show_number(episode):
    show_number = episode["show_number"]
    show_number = show_number.replace("#", "")
    episode["show_number"] = int(show_number)
    return episode


def convert_to_datetime(dt_str, dt_format, timezone="PST"):
    converted_dt = datetime.datetime.strptime(dt_str, dt_format)
    # converted_dt.replace(tzinfo=pytz.timezone(timezone))
    return converted_dt.date()


@logger.catch
def sort_reference_links(episode):
    references = episode["reference_list"]
    references = remove_none_from_list(references)
    github_references = []
    github_reference_count = 0

    for reference in references:
        if "github.com" in reference["url"]:
            github_references.append(reference)
            github_reference_count += 1

    episode["github_references"] = github_references
    episode["github_reference_count"] = github_reference_count

    return episode


@logger.catch
def clean_episode(episode):
    if episode is None:
        logger.warning(f"Can not clean NoneType, returning None.")
        return None
    episode = clean_episode_dates_info(episode)
    episode = clean_episode_show_number(episode)
    episode = sort_reference_links(episode)

    try_to_save_to_pickle(
        data=episode,
        filename=f"data/episodes/cleaned_episode_{episode['show_number']}.pk",
    )
    logger.info(f"Cleaned Episode {episode}")
    return episode


def get_if_not_pickled(filename, get_content, *args, **kwargs):
    try:
        return get_from_pickle(filename)
    except FileNotFoundError:
        kwargs["picklename"] = filename
        return get_content(*args, **kwargs)


def get_random_wait_time():
    wait_time = abs(0.5 * (np.random.poisson() - 1))
    logger.debug(f"Wait for {wait_time}s ...")
    return wait_time


def remove_none_from_list(list_):
    length = len(list_)
    try:
        while True:
            list_.remove(None)
    except ValueError:
        pass

    logger.info(f"Removed {length - len(list_)} null entries from list.")

    return list_


@logger.catch
def get_all_episodes(podcast_info):
    logger.info(f"Get all Episodes for {podcast_info}.")

    data = try_to_load_from_pickle(**podcast_info)
    if data:
        data = remove_none_from_list(data)
        return data, True

    episode_list, pickled = get_episode_list(**podcast_info)
    cleaned_episode_list = []

    for entry in episode_list:
        episode, pickled = get_mentioned_links_for_episode(entry)
        if episode is None:
            continue
        if not pickled:
            episode = clean_episode(episode)
        episode["reference_list"] = remove_none_from_list(episode["reference_list"])
        episode["github_references"] = remove_none_from_list(
            episode["github_references"]
        )
        cleaned_episode_list.append(episode)

    cleaned_episodes = remove_none_from_list(cleaned_episode_list)

    try_to_save_to_pickle(data=cleaned_episodes, **podcast_info)

    logger.success(f"Got all Episodes.")
    return cleaned_episodes, False

