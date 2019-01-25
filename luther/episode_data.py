import re
from loguru import logger

from .github_data import Repository, StarGazer
from .base import LutherBaseClass
from .get_github_data import get_raw_repository_info


_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")

class Reference(LutherBaseClass):
    def __init__(self, **ref_data):
        self.text = ref_data.get("text")
        self.url = ref_data.get("url")
        self.episode_number = ref_data.get("episode_number", None)
        self.episode_title = ref_data.get("episode_title", None)
        self.date_referenced = ref_data.get("date_referenced", None)

        # Add Episode data
        self._is_github_ref = "github" in self.url
        self.extract_rep_info_from_url()

        # Add Repository data
        repository = ref_data.get("repository", None)

        if isinstance(repository, Repository):
            self.repository = repository
        else:
            self.repository = None

        super().__init__(**ref_data)

        if self._is_github_ref and self.repository is None:
            self.create_repository()

    def __hash__(self):
        return hash((self.text, self.url, self._is_github_ref))

    def extract_rep_info_from_url(self):
        if self._is_github_ref:
            logger.debug(
                f"Extracting owner and name of github ({self._is_github_ref}) repo: {self.url}, {self}"
            )
            try:
                rep_match = re.search(
                    r".*github.com/(?P<rep_owner>[a-zA-Z0-9-._]*)/(?P<rep_name>[a-zA-Z0-9-._]*).*",
                    self.url,
                )
                self.rep_owner = rep_match.group("rep_owner")
                self.rep_name = rep_match.group("rep_name")
            except AttributeError as e:
                logger.warning(
                    f"Could not get repository_owner and repository_name from {self.url}. Marking {self} as not github_ref."
                )
                self._is_github_ref = False
                logger.error(e)
                return None
            logger.success(f"Extracted Repository_info")
        return self

    @property
    def unique_id(self):
        _id = (
            str(self.episode_number)
            + "_"
            + str(self._id)
            + "_"
            + str(self._parent_uuid)
        )
        return _id

    def __eq__(self, other):
        self_ = (self.repository, self._id, self.episode_number, self.url)
        other_ = (other.repository, other._id, other.episode_number, other.url)
        return self_ == other_

    def __lt__(self, other):
        self_ = (self.repository, self._id, self.episode_number, self.url)
        other_ = (other.repository, other._id, other.episode_number, other.url)
        return self_ < other_

    def __repr__(self):
        return f"Reference(episode_number={self.episode_number}, is_github_ref={self._is_github_ref}, url={self.url})"

    def get_repo_data(self):
        return {"rep_owner": self.rep_owner, "rep_name": self.rep_name}

    @logger.catch
    def create_repository(self):
        logger.info(f"Creating Repository for {self}.")
        raw_repo_info = get_raw_repository_info(**self.get_repo_data())
        try:
            raw_repo_info["_parent_uuid"] = str(self._uuid)
            repository = Repository(**raw_repo_info)
        except TypeError as e:
            if raw_repo_info is None:
                logger.warning(
                    f"Encountered a NoneType in raw_repo_info: {raw_repo_info}."
                )
            logger.warning(f"Can not create repository for {self}. Check manually.")
            logger.error(e)
            return self

        self.repository = repository
        logger.success(f"Created {self}")
        return self


class Episode(LutherBaseClass):
    def __init__(self, **episode_data):
        self.number = episode_data.get("show_number")
        self.title = episode_data.get("title")
        self.episode_url = episode_data.get("episode_url", "")
        self.guest_host = episode_data.get("guests")
        self.date_recorded = episode_data.get("date_recorded")
        self.date_published = episode_data.get("date_published")
        self.references = []
        self.github_references = []
        self.github_reference_count = episode_data.get("github_reference_count", 0)

        super().__init__(**episode_data)
        if "reference_list" in episode_data:
            self.append_raw_references(episode_data["reference_list"])
        
        logger.success(f"Created {self}")

    def __repr__(self):
        return f"Episode(number={self.number}, title={self.title})"

    def __hash__(self):
        return hash(
            (
                self.number,
                self.title,
                self.episode_url,
                self.date_recorded,
                self.date_published,
            )
        )

    def __eq__(self, other):
        return (self.number, self.date_published, self.date_recorded) == (
            other.number,
            other.date_published,
            other.date_recorded,
        )

    def __lt__(self, other):
        return (self.number, self.date_published, self.date_recorded) < (
            other.number,
            other.date_published,
            other.date_recorded,
        )

    @classmethod
    def validate_references_type(cls, references):
        if not isinstance(references, list):
            logger.warning(f"Ignoring references. Not of type list.")
            return None

        for reference in references:
            if not isinstance(reference, Reference):
                logger.warning(
                    f"Ignoring reference={reference} is not instance of Reference."
                )
                references.remove(reference)

        return references

    @property
    def unique_id(self):
        title = self.title.strip().lower().replace(" ", "_")
        return title + "_" + "#" + str(self.number)

    @logger.catch
    def remove_duplicate_references(self):
        old_count = self.reference_count
        try:
            self.references = list(set(self.references))
            if self.reference_count != old_count:
                logger.warning(
                    f"Removed {old_count - self.reference_count} duplicate references."
                )
        except TypeError as e:
            logger.error(
                f"remove_duplicate_references TypeError in {self}, specifically in {self.references}."
            )
            raise e

        return self

    def clean(self):
        self.remove_duplicate_references()
        self.is_clean = True
        return self

    @property
    def reference_count(self):
        return len(self.references)

    def append_references(self, references):
        references = Episode.validate_references_type(references)
        self.references += references
        return self.remove_duplicate_references()

    def append_raw_references(self, raw_references):
        logger.info(f"Create and Append References from raw data.")
        episode_data = {
            "episode_number": self.number,
            "episode_title": self.title,
            "date_referenced": self.date_published,
            "_parent_uuid": self._uuid,
        }
        if not isinstance(raw_references, list):
            raw_reference = {**raw_references, **episode_data}
            reference = Reference.create_from_dict(**raw_reference)
            self.references.append(reference)
            return self.remove_duplicate_references()

        references = Reference.create_from_list(raw_references, episode_data)
        return self.append_references(references)


class Podcast(LutherBaseClass):
    def __init__(self, **pod_data):
        self.author = pod_data.get("author", "")
        self.name = pod_data.get("name")
        self.url = pod_data.get("url")
        self.initial_start_date = pod_data.get("initial_start_date")
        episodes = pod_data.get("episodes", [])
        self.episodes = Podcast.validate_episodes_type(episodes)
        self.exportable_data_rows = []

        super().__init__(**pod_data)
        logger.success(f"Created {self}")

    def __repr__(self):
        return f"Podcast(author={self.author}, url={self.url}, episode_count={self.episode_count})"

    def __hash__(self):
        return hash((self.author, self.name, self.url))

    @classmethod
    def validate_episodes_type(cls, episodes):
        if not isinstance(episodes, list):
            logger.warning(f"Ignoring episodes. Not of type list.")
            return None

        for episode in episodes:
            if not isinstance(episode, Episode):
                logger.warning(
                    f"Ignoring episode={episode} is not instance of Episode."
                )
                episodes.remove(episode)

        return episodes

    def remove_duplicate_episodes(self):
        old_eps_count = self.episode_count
        self.episodes = list(set(self.episodes))
        if self.episode_count != old_eps_count:
            logger.warning(
                f"Removed {old_eps_count - self.episode_count} duplicate episodes."
            )

        return self

    @property
    def unique_id(self):
        author = self.author.strip().lower().replace(" ", "_")
        name = self.name.strip().lower().replace(" ", "_")
        return author + "_" + name

    @property
    def episode_count(self):
        return len(self.episodes)

    def append_episodes(self, episodes):
        episodes = Podcast.validate_episodes_type(episodes)
        self.episodes += episodes
        return self.remove_duplicate_episodes()

    def append_raw_episodes(self, raw_episodes):
        logger.info(f"Create and Append Episodes from raw data.")
        if raw_episodes is None:
            logger.warning(
                f"Encountered a NoneType while appending raw episodes to {self}"
            )
            return self
        podcast_info = {"_parent_uuid": self._uuid}
        if not isinstance(raw_episodes, list):
            episode = Episode.create_from_dict(**{**raw_episodes, **podcast_info})
            self.episodes.append(episode)
            return self.remove_duplicate_episodes()

        episodes = Episode.create_from_list(raw_episodes, podcast_info)
        return self.append_episodes(episodes)
