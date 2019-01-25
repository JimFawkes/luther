import datetime
import pytz
from loguru import logger
import pickle
from .get_github_data import get_raw_stargazer_info
from .base import LutherBaseClass

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")

GITHUB_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class NotGitHubType(TypeError):
    pass


class DataValidationError(Exception):
    pass



class StarGazer(LutherBaseClass):
    def __init__(self, **sg_data):
        """Read GitHub GraphQL StarGazer data.
        """
        self.date_starred = sg_data.get("starredAt")
        sg_node = sg_data.get("node")
        self.user_id = sg_node.get("id")
        self.user_name = sg_node.get("name")
        self.user_url = sg_node.get("url")

        self.repository_name = sg_data.get("repository_name")
        self.repository_owner = sg_data.get("repository_owner")
        self.repository_url = sg_data.get("repository_url")
        self._read_from_storage = sg_data.get("read_from_storage", False)

        self.date_starred = datetime.datetime.strptime(
            self.date_starred, GITHUB_DATETIME_FORMAT
        ).replace(tzinfo=pytz.utc).date()

        super().__init__(**sg_data)

    def __repr__(self):
        return f"StarGazer(date_starred={self.date_starred}, user_name={self.user_name}, repository_name={self.repository_name})"

    def __hash__(self):
        return hash(
            (
                self._date_requested,
                self.date_starred,
                self.user_id,
                self.user_name,
                self.user_url,
                self.repository_name,
            )
        )

    def __lt__(self, other):
        self_ = (self.date_starred, self.repository_url, self.user_id, self._date_requested)
        other_ = (other.date_starred, other.repository_url, other.user_id, other._date_requested)
        return self_ < other_

    def __eq__(self, other):
        self_ = (self.date_starred, self.repository_url, self.user_id, self._date_requested)
        other_ = (other.date_starred, other.repository_url, other.user_id, other._date_requested)
        return self_ == other_

class Repository(LutherBaseClass):
    def __init__(self, **repo_data):
        """Read the GitHub GraphQL repository data.
        """

        # Break if any of these keys are not in repo_data
        self.full_name = repo_data.get("nameWithOwner")
        self.url = repo_data.get("url")
        self.date_created = repo_data.get("createdAt")
        self.stargazer_count = int(repo_data.get("stargazers").get("totalCount"))

        # Be graceful if any of these keys is missing
        self.watcher_count = int(repo_data.get("watchers", {}).get("totalCount", -1))
        self.primary_language = repo_data.get("primaryLanguage", None)
        self.is_fork = bool(repo_data.get("isFork", None))
        self.languages = [
            lang.get("name", None)
            for lang in repo_data.get("languages", {}).get("nodes", [{}])
        ]
        self.fork_count = int(repo_data.get("forkCount", -1))
        self.owner_id = repo_data.get("owner", {}).get("id", None)

        # None GitHub Repo Data
        self.mentioned_in_podcast = repo_data.get("mentioned_in_podcast", False)

        # Manipulated fields
        self.owner, self.name = self.full_name.split("/")
        self.date_created = datetime.datetime.strptime(
            self.date_created, GITHUB_DATETIME_FORMAT
        ).replace(tzinfo=pytz.utc).date()

        self.stargazers = []

        super().__init__(**repo_data)

        if 0 < self.stargazer_count:
            logger.info(f"Create stargazers for {self}")
            stargazers = self.create_stargazers()
            self.append_stargazers(stargazers)

        self.pickle()

        # Check if clean
        logger.info(f"Created {self}")
        logger.success(f"Created {self}")

    def __repr__(self):
        return f"Repository(url='{self.url}')"

    def __hash__(self):
        return hash(
            (
                self._date_requested,
                self.date_created,
                self.full_name,
                self.url,
                self.stargazer_count,
                self.watcher_count,
            )
        )

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    @property
    def unique_id(self):
        _id = self.full_name.strip().replace('/', '_')
        _id += str(self._id) + '_' + str(self._uuid)
        return _id

    def create_stargazers(self):
        stargazer_data = get_raw_stargazer_info(
            **{"rep_owner": self.owner, "rep_name": self.name}
        )

        logger.info(f"Convert {len(stargazer_data)} data sets to StarGazer objects.")
        stargazers = []
        for stargazer in stargazer_data:
            stargazers.append(StarGazer(**{**stargazer, **self.repository_info}))

        return stargazers

    @property
    def date_requested(self):
        return self._date_requested

    def remove_duplicate_stargazers(self):
        stargazers_set = set(self.stargazers)
        cleaned_stargazers = list(stargazers_set)

        if len(cleaned_stargazers) != len(self.stargazers):
            logger.warning("Removed duplicate StarGazers from {self}.")

        self.stargazers = cleaned_stargazers

        return self

    def append_stargazers(self, stargazers):
        if not isinstance(stargazers, list):
            raise TypeError(
                "Repository.add_stargazers requires a list of StarGazer objects."
            )
        if not isinstance(stargazers[0], StarGazer):
            raise NotGitHubType(
                "Repository.add_stargazers requires a list of StarGazer objects."
            )

        try:
            self.stargazers += stargazers
        except AttributeError:
            self.stargazers = stargazers

        self.remove_duplicate_stargazers()

        if self.added_all_stargazers:
            logger.info(f"All StarGazers have been added to {self}.")
        elif self.stargazer_count < len(stargazers):
            logger.error(
                f"Added more StarGazers than the total count. Check for duplicates."
            )
            raise DataValidationError(f"Check {self} for duplicates.")

        logger.success(f"Added StarGazers to {self}")
        return self

    @property
    def repository_info(self):
        """Get a dict to pass along to StarGazer Constructors.
        """

        return {
            "repository_name": self.name,
            "repository_owner": self.owner,
            "repository_url": self.url,
            "_parent_uuid": self._uuid,
        }

    @property
    def added_all_stargazers(self):
        return len(self.stargazers) == self.stargazer_count
