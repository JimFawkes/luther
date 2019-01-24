import uuid
import datetime
import pickle
import time
import numpy as np
import pandas as pd
import pytz

from github_data import Repository, StarGazer
from episode_data import Podcast, Episode, Reference
import scrape_tptm as stptm

from loguru import logger


_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success_{_log_file_name}.log", rotation="1 day", level="SUCCESS")
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")


def get_timestamp():
    return datetime.datetime.utcnow().strftime(format="%Y%m%d_%H%M")

class LutherDataRow:
    def __init__(self, **data):
        self.date = data.get('date')
        self.star_count_accu = data.get('star_count_accu')
        self.star_count_diff = data.get('star_count_diff')
        self.star_count_rel = data.get('star_count_rel')
        self.date_mentioned = data.get('date_mentioned')
        self.date_repository_created = data.get('date_repository_created')
        self.podcast_name = data.get('podcast_name')
        self.podcast_start_date = data.get('podcast_start_date')
        self.episode_number = data.get('episode_number')
        self.episode_title = data.get('episode_title')
        self.repository_url = data.get('repository_url')
        self.repository_is_fork = data.get('repository_is_fork')
        self.repository_primary_language = data.get('repository_primary_language')
        self.repository_name = data.get('repository_name')
        self.repository_owner = data.get('repository_owner')
        self.repository_exists = data.get('repository_exists', True)
        self.manually_modified = data.get('_manually_modified', False)
        self.date_requested_repository_data = data.get('date_requested')
        self._uuid = str(uuid.uuid4())
        self.days_since_data_requested = data.get('days_since_data_requested')

        self.days_since_creation = (self.date - self.date_repository_created).days
        self.days_since_mention = (self.date - self.date_mentioned).days
        self.days_since_podcast_start = (self.date - self.podcast_start_date).days

    def __repr__(self):
        return f"LutherDataRow(date={self.date}, star_count={self.star_count})"

    def __lt__(self, other):
        self_ = (self.date, self.star_count)
        other_ = (other.date, other.star_count)
        return self_ < other_

    def __eq__(self, other):
        self_ = (self.date, self.star_count, self.episode_number, self.episode_title, self.podcast_name)
        other_ = (other.date, other.star_count, other.episode_number, other.episode_title, other.podcast_name)
        return self_ == other_

    def get_keys_to_exclude_from_export(self):
        exclude = ['_uuid',]
        return exclude
    
    def get_usable_dict(self):
        """Return dict with all attrs but exclude previously defined keys."""
        luther_dict = self.__dict__.copy()
        exclude_keys = self.get_keys_to_exclude_from_export()
        for exclude_key in exclude_keys:
            luther_dict.pop(exclude_key)
        return luther_dict

    @property
    def column_names(self):
        return self.get_usable_dict().keys()

    @property
    def values(self):
        return tuple(self.get_usable_dict().values())
        
    @property
    def all_data(self):
        return self.__dict__.copy()

def check_manually_modified(row_data, inst):
    try:
        result = row_data['_manually_modified'] = inst._manually_modified
        return result
    except AttributeError:
        logger.warning(f"Instance {inst} has no attr _manually_modified. Using False.")
        return False

def make_datetime2date(dt):
    try:
        return dt.date()
    except AttributeError:
        return dt

@logger.catch
def convert_podcast_to_luther_datarows(podcast, rows=[]):
    """Convert/Flatten Data from its Hirarchical Structure to usable/flat rows.

    WARNING: This monstrosity of a function should only be run with caution.
    Due to too many for-loops this might take significant amounts of time.

    This beeing said, the actual runtime for the current usecase was only roughly one minute.
    This measurement was made for the following sizes:
        1x Podcast
            191x Episodes
                1707x References
                    129x Repositories
                        402655x StarGazers
    
    Resulting in 247181x Rows.
    Final Output: Finished converting data into data_rows. Total Row Count: 247181, Total Execution Time: 129.57409000396729

    Run on JupyterNotebook with Python 3.6

    # REFACTOR: Break this down into simpler and more efficient steps/function/s.

    """

    luther_data_rows = []
    start = time.time()
    logger.info(f"Converting podcast data into usable data_rows.")
    row_data = {}
    podcast_start_date = podcast.initial_start_date
    row_data['podcast_name'] = podcast.name
    row_data['podcast_start_date'] = podcast_start_date
    row_data['_manually_modified'] = podcast._manually_modified
    for episode in podcast.episodes:
        row_data['_manually_modified'] = check_manually_modified(row_data, episode)
        row_data['episode_number'] = episode.number
        row_data['episode_title'] = episode.title
        row_data['date_mentioned'] = episode.date_published
        for reference in episode.references:
            row_data['_manually_modified'] = check_manually_modified(row_data, reference)
            if reference.repository is not None:
                repository = reference.repository
                repository.date_created = make_datetime2date(repository.date_created)
                repository._date_requested = make_datetime2date(repository._date_requested)
                row_data['_manually_modified'] = check_manually_modified(row_data, repository)
                row_data['repository_is_fork'] = repository.is_fork
                row_data['repository_owner'] = repository.owner
                row_data['repository_name'] = repository.name
                row_data['date_requested'] = repository._date_requested # .date()
                try:
                    row_data['repository_primary_language'] = repository.primary_language['name']
                except (TypeError, AttributeError):
                    logger.warning(f"Encountered NoneType for primary_language field in {repository}. Setting to np.nan.")
                    row_data['repository_primary_language'] = np.nan
                row_data['repository_url'] = repository.url
                row_data['date_repository_created'] = repository.date_created
                date_mapping = []
                days_since_repository_was_created = (repository._date_requested - repository.date_created).days
                # Expect one years worth of data before the first podcast was aired, for every repository. Fill missing info with zeros.
                minimum_date_records = 365 + (repository._date_requested - podcast_start_date).days 
                if days_since_repository_was_created < minimum_date_records:
                    days_since_repository_was_created = minimum_date_records
                for i in range(days_since_repository_was_created-1, -1, -1):
                    date_ = (datetime.datetime.utcnow() - datetime.timedelta(days=i)).date()
                    date_mapping.append({'date': date_, 'star_count_accu': 0, 'star_count_diff': 0, 'repository_exists':True, 'days_since_data_requested':-i})
                logger.info(f"Created {len(date_mapping)} date entries for {repository}.")
                for stargazer in repository.stargazers:
                    # row_data['_manually_modified'] = check_manually_modified(row_data, stargazer)
                    for date in date_mapping:
                        if stargazer.date_starred <= date['date']:
                            date['star_count_accu'] += 1
                        if stargazer.date_starred == date['date']:
                            date['star_count_diff'] += 1
                logger.info(f"Updated star_count for date entries. First: {date_mapping[0]}, Last: {date_mapping[-1]}.")
                prev_total_star_count = 0
                for date in date_mapping:
                
                    if date['date'] < repository.date_created:
                        date['repository_exists'] = False
                    try:
                        date['star_count_rel'] = date['star_count_diff'] / prev_total_star_count
                    except ZeroDivisionError:
                        date['star_count_rel'] = 0
                    luther_data_row = LutherDataRow(**{**row_data, **date})
                    # podcast.exportable_data_rows.append(luther_data_row)
                    luther_data_rows.append(luther_data_row)
                    rows.append(luther_data_row.values)
                    prev_total_star_count = date['star_count_accu']

                logger.info(f"Current Row Count is {len(rows)}, current execution time: {time.time() - start}")
    columns = luther_data_rows[0].column_names
    podcast.exportable_data_rows = luther_data_rows
    podcast.pickle()
    logger.info(f"Finished converting data into data_rows. Total Row Count: {len(rows)}, Total Execution Time: {time.time() - start}.")
    logger.success(f"Finished converting data into data_rows. Total Row Count: {len(rows)}, Total Execution Time: {time.time() - start}.")
    return (rows, columns, podcast)
                

@logger.catch
def convert_rows_to_dataframe(rows, columns, podcast, filename=None):
    logger.info(f"Converting {len(rows)} rows into a pandas.DataFrame.")
    df = pd.DataFrame(rows)
    df.columns = columns
    logger.info(f"Created DataFrame with shape: {df.shape} and columns: {df.columns}.")
    if filename is None:
        filename = podcast.name.strip().lower().replace(' ', '_')
        filename = filename.replace('/', '_')
        filename = filename.replace('.', '_')
        filename = 'data/dataframe/' + filename + '.pk'

    with open(filename, 'wb') as f:
        pickle.dump(df, f)
    logger.info(f"Pickled dataframe to {filename}.")

    logger.success(f"Finished converting {len(rows)} for {podcast.name} into pd.DataFrame.")
    logger.success(f"Pickled dataframe to {filename}.")

    return df

@logger.catch
def get_multiple_podcasts():
    logger.info(f"Get Data for multiple Podcasts")
    tptm_podcast_info = {
        "author": "Michael Kennedy",
        "name": "Talk Python To Me",
        "url": "https://talkpython.fm/episodes/all",
        "initial_start_date": datetime.datetime(2015, 3, 21, 12, 30, tzinfo=pytz.utc).date(),
        "filename": "data/podcast_talk_python_to_me_data.pk",
    }

    pb_podcast_info = {
        "author": "Michael Kennedy, Brian Okken",
        "name": "PythonBytes",
        "url": "https://pythonbytes.fm/episodes/all",
        "initial_start_date": datetime.datetime(2016, 11, 5, 12, 30, tzinfo=pytz.utc).date(),
        "filename": "data/podcast_python_bytes_data.pk",
    }
    podcasts_info = [tptm_podcast_info, pb_podcast_info]
    podcasts = []

    for podcast_info in podcasts_info:
        podcast = get_podcast_data(podcast_info)
        podcasts.append(podcast)

    logger.success(f"Got multiple Podcasts.")
    return podcasts

@logger.catch
def get_podcast_data(podcast_info):
    logger.info(f"Create Podcast instance for {podcast_info['name']} podacast.")

    # TODO: Get from pickle or create
    podcast = Podcast(**podcast_info)

    logger.info(f"Get all podcast episodes.")
    raw_episode_data, pickled = stptm.get_all_episodes(podcast_info)

    logger.info(f"Create and Append all Episode instances from raw episode data.")
    podcast.append_raw_episodes(raw_episode_data)

    logger.info(f"Pickle the entire {podcast.name}")
    podcast.pickle()

    logger.success(f"Got Data for {podcast.name}")
    return podcast

@logger.catch
def convert_podcast_to_pd_df(podcast):
    logger.info(f"Convert {podcast.name} to DataFrame")
    luther_data_rows = []
    rows, columns, podcast = convert_podcast_to_luther_datarows(podcast, luther_data_rows)

    df = convert_rows_to_dataframe(rows, columns, podcast)

    logger.success(f"Converted {podcast.name} to DataFrame")
    return df

@logger.catch
def clean_df(df, days_premention=365, days_postmention=30):
    logger.info(f"Clean DataFrame")
    clean_df = df
    clean_df['date_mentioned'] = pd.to_datetime(clean_df['date_mentioned'])
    exclude_new_episodes = (clean_df['date_mentioned'] < datetime.datetime.utcnow() - datetime.timedelta(days=31))
    clean_df = clean_df[exclude_new_episodes]
    time_frame = ((clean_df['days_since_mention'] > -(days_premention + 1)) & (clean_df['days_since_mention'] < days_postmention + 1))
    clean_df = df[time_frame]
    clean_df['dsm_off'] = clean_df['days_since_mention'] + days_premention
    clean_df['fake_date'] = (pd.to_datetime(datetime.datetime(2019, 1, 1, 12,30) - datetime.timedelta(days=days_premention)).date())
    clean_df['fake_date'] = clean_df['fake_date'] + clean_df['dsm_off'].apply(pd.offsets.Day)
    clean_df = pd.concat([clean_df, pd.get_dummies(clean_df['repository_primary_language'])], axis=1)
    logger.success(f"Cleaned DataFrame.")
    return clean_df

@logger.catch
def partition_timeseries_podcast_data(clean_df, filename_prefix=''):
    """Seperate the dataframe into three groups, Test, Validation and Training.

        clean_df: input a complete and clean dataframe.

        return training_df, validation_df

        1. Test Data -> New DF pickled and NOT returned.
        2. Validation Data -> New DF, pickled and returned.
        3. Training Data -> New DF, pickled and returned.

        The pickled dataframes will be stored in the data/dataframe 
        directory.

        NOTE: Since this is the final step, clean and complete data is assumed as input.

        How is partitioned?
        The data consits of multiple podcasts with multiple episodes.
        Because it is assumed, that the audience of every podcast has grown over time, we take (as for normal timeseries data)
        the most recent 20% for test data and the most recent 25% of the remaining 80% as validation data.

        We partition by taking these percentages of episodes (with mentioned GH Repositories) for every podcast and concatinate
        the resulting dfs.
        """
    test_dfs = []
    validation_dfs = []
    training_dfs = []
    logger.info(f"Start Partition Timeseries for Podcast Data")
    podcast_names = clean_df.podcast_name.unique()
    for podcast_name in podcast_names:
        logger.info(f"Partition data for Podcast: {podcast_name}.")
        podcast_df = clean_df[clean_df['podcast_name'] == podcast_name]
        episodes = sorted(clean_df['episode_number'].unique())
        episode_count = len(episodes)
        episode_cutoff = int(episode_count * 0.20)
        logger.debug(f"EP Count: {episode_count}, EP Cutoff: {episode_cutoff}")

        first_test_ep = sorted(episodes[-episode_cutoff:])[0]
        validation_episodes = sorted(episodes[-(2*episode_cutoff):-episode_cutoff])
        first_validation_ep, last_validation_ep = validation_episodes[0], validation_episodes[-1]
        last_training_ep = sorted(episodes[:-(2*episode_cutoff)])[-1]
        # Get the most recent 20% of episodes for the test data
        logger.info(f"First Test Episode is: {first_test_ep}.")
        test_dfs.append(podcast_df[podcast_df['episode_number'] >= first_test_ep])
        logger.info(f"Validation Episodes are, First: {first_validation_ep}, Last: {last_validation_ep}.")
        validation_dfs.append(podcast_df[(podcast_df['episode_number'] >= first_validation_ep) & (podcast_df['episode_number'] <= last_validation_ep)])
        logger.info(f"Last Training Episode: {last_training_ep}.")
        training_dfs.append(podcast_df[(podcast_df['episode_number'] <= last_training_ep)])

    test = pd.concat(test_dfs)
    validation = pd.concat(validation_dfs)
    training = pd.concat(training_dfs)

    timestamp = get_timestamp()

    test_filename = 'data/dataframe/_' + filename_prefix + '_TEST_' + timestamp + '.pk'
    validation_filename = 'data/dataframe/' + filename_prefix + '_VALIDATION_' + timestamp + '.pk'
    training_filename = 'data/dataframe/' + filename_prefix + '_training_' + timestamp + '.pk'

    with open(test_filename, 'wb') as f:
        pickle.dump(test, f)
        logger.success(f"Pickled test_df to {test_filename}.")

    with open(validation_filename, 'wb') as f:
        pickle.dump(validation, f)
        logger.success(f"Pickled validation_df to {validation_filename}.")

    with open(training_filename, 'wb') as f:
        pickle.dump(training, f)
        logger.success(f"Pickled validation_df to {training_filename}.")

    return training, validation


@logger.catch
def run_all():
    logger.info(f"Run All")

    podcasts = get_multiple_podcasts()

    clean_dfs = []
    for podcast in podcasts:
        df = convert_podcast_to_pd_df(podcast)
        clean_dfs.append(clean_df(df, days_premention=366))

    clean = pd.concat(clean_dfs)

    with open('', 'wb') as f:
        pickle.dump(clean, f)

    training, validation = partition_timeseries_podcast_data(clean)

    return training, validation