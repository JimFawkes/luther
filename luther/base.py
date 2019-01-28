import datetime
import pytz
import pickle
import uuid

from loguru import logger

_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")


class LutherBaseClass:
    def __init__(self, **data):

        self._id = data.get("_id", -1)
        self._uuid = data.get("_uuid", str(uuid.uuid4()))
        self._parent_uuid = data.get("_parent_uuid", None)
        self._manually_modified = data.get("_manually_modified", False)

        self._read_from_storage = data.get("read_from_storage", False)
        if "filename" in data:
            self.filename = data.get("filename")

        if self._read_from_storage:
            self._date_requested = data.get("_date_requested")
        else:
            self._date_requested = (
                datetime.datetime.utcnow().replace(tzinfo=pytz.utc).date()
            )

        if not hasattr(self, "is_clean"):
            self.is_clean = False

    def __hash__(self):
        return hash(self._read_from_storage, self._date_requested)

    @classmethod
    def create_from_dict(cls, **raw_dict):
        inst = cls(**raw_dict)
        _ = inst.clean()
        inst.pickle(is_raw=False)
        return inst

    @classmethod
    def create_from_list(cls, raw_list, additional_data={}):
        created_list = []
        for idx, raw_dict in enumerate(raw_list):
            additional_data["_id"] = idx
            combined_dict = {**raw_dict, **additional_data}
            created_list.append(cls.create_from_dict(**combined_dict))

        return created_list

    @classmethod
    def unpickle(cls, filename):
        with open(filename, "rb") as f:
            obj = pickle.load(f)

        if not isinstance(obj, cls):
            raise TypeError(
                f"Unpickled obj is of type {type(obj)}, expected type {cls}."
            )

        return obj

    @property
    def unique_id(self):
        return str(abs(self.__hash__()))

    def clean(self):
        return self

    def _get_filename(self):
        if hasattr(self, "filename"):
            return self.filename

        name = type(self).__name__.lower()

        filename = "data/" + name + "/"
        filename += "clean" if self.is_clean else "pre_clean"
        filename += "_" + name + "_" + self.unique_id
        return filename

    def pickle(self, filename=None, is_raw=False):

        filename = self._get_filename()
        filename += "_raw.pk" if is_raw else "_instance.pk"

        with open(filename, "wb") as f:
            pickle.dump(self, f)

        logger.info(f"Pickled {self} to file {filename}.")
