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
import time
import pickle
import datetime
import pytz
import re
import numpy as np

from loguru import logger

import os

logger.add("logs/scrape_tptm_{time}.log", rotation="100 MB")


TPTM_BASE_URL = "https://talkpython.fm"
TPTM_EPISODES_URL = "https://talkpython.fm/episodes/all"

chromedriver = "/Applications/chromedriver"  # path to the chromedriver executable
os.environ["webdriver.chrome.driver"] = chromedriver


def get_driver():
    return webdriver.Chrome(chromedriver)


def save_to_pickle(data, filename):
    with open(filename, "wb") as f:
        pickle.dump(data, f)


def get_from_pickle(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


@logger.catch
def try_to_load_from_pickle(load_from_pickle=True, filename=None, **kwargs):
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
def get_episode_list(driver, **kwargs):

    data = try_to_load_from_pickle(**kwargs)
    if data:
        return data

    driver.get(TPTM_EPISODES_URL)

    try:
        episodes_table = driver.find_elements_by_class_name("episodes")
        episodes_table_body = episodes_table[1].find_element_by_tag_name("tbody")
        episode_rows = episodes_table_body.find_elements_by_tag_name("tr")
    except NoSuchElementException as e:
        logger.error(e)
        return None

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

    try_to_save_to_pickle(entries_list, **kwargs)

    return entries_list


@logger.catch
def get_mentioned_links_for_episode(driver, episode, **kwargs):

    data = try_to_load_from_pickle(
        filename=f"data/episodes/cleaned_episode_{episode['show_number']}.pk"
    )
    if data:
        return data

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
        return None

    reference_list = []
    for reference in episode_references:
        reference_list.append(
            {"text": reference.text, "url": reference.get_property("href")}
        )

    episode["dates_info"] = episode_dates_info
    episode["reference_list"] = reference_list
    logger.info(
        f"Got info for episode {episode['show_number']} with idx: {episode['idx']}, named: {episode['title']}"
    )
    return episode


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

    try_to_save_to_pickle(
        data=episode,
        filename=f"data/episodes/cleaned_episode_{episode['show_number']}.pk",
    )

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


def sort_reference_links(episode):
    references = episode["reference_list"]
    github_references = []
    github_reference_count = 0

    for reference in references:
        if "github.com" in reference["url"]:
            github_references.append(reference)
            github_reference_count += 1

    episode["github_references"] = github_references
    episode["github_reference_count"] = github_reference_count

    return episode


def clean_episode(episode):
    if not episode:
        return None
    episode = clean_episode_dates_info(episode)
    episode = clean_episode_show_number(episode)
    episode = sort_reference_links(episode)
    return episode


def get_if_not_pickled(filename, get_content, *args, **kwargs):
    try:
        return get_from_pickle(filename)
    except FileNotFoundError:
        kwargs["picklename"] = filename
        return get_content(*args, **kwargs)


def get_random_wait_time():
    return 0.5 * (np.random.poisson() - 1)


@logger.catch
def get_all_episodes(filename="data/talk_python_to_me_episode_data_clean.pk"):

    data = try_to_load_from_pickle(filename=filename)
    if data:
        return data

    driver = get_driver()
    episode_list = get_episode_list(driver, filename="data/episode_list.pk")
    cleaned_episode_list = []

    for entry in episode_list:
        get_random_wait_time()
        episode = get_mentioned_links_for_episode(driver, entry)
        episode = clean_episode(episode)
        cleaned_episode_list.append(episode)

    try_to_save_to_pickle(data=cleaned_episode_list, filename=filename)

    return cleaned_episode_list
