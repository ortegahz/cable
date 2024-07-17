import glob
import os.path

from data.data import *
from utils.utils import make_dirs


class ParserBase:
    def __init__(self, db_type, dir_plot_save):
        self.db_type = db_type
        self.dir_plot_save = dir_plot_save
        make_dirs(self.dir_plot_save, reset=True)

    def parse(self):
        raise NotImplementedError


class ParserV0(ParserBase):
    """
    format: <path>
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, dir_plot_save)
        self.path_in = addr_in

    def parse(self):
        _basename = os.path.basename(self.path_in)
        _name, _ = os.path.splitext(_basename)
        logging.info(self.path_in)
        db_obj = eval(self.db_type)(self.path_in)
        db_obj.load()
        db_obj.plot(dir_save=self.dir_plot_save, show=True, save_name_prefix=_name)
        db_obj.save_to_csv(os.path.join(self.dir_plot_save, _name + '.csv'))


class ParserV0CSV(ParserBase):
    """
    format: <path>
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, dir_plot_save)
        self.path_in = addr_in

    def parse(self):
        _basename = os.path.basename(self.path_in)
        _name, _ = os.path.splitext(_basename)
        logging.info(self.path_in)
        db_obj = eval(self.db_type)(self.path_in)
        db_obj.load()
        db_obj.plot(dir_save=self.dir_plot_save, show=True, save_name_prefix=_name)


class ParserV1(ParserBase):
    """
    format: dir/<paths>
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, dir_plot_save)
        self.dir_in = addr_in

    def _get_filtered_paths(self):
        txt_paths = glob.glob(os.path.join(self.dir_in, '*.txt'))
        log_paths = glob.glob(os.path.join(self.dir_in, '*.log'))
        return txt_paths + log_paths

    def parse(self):
        _paths = self._get_filtered_paths()
        logging.info(_paths)
        for _path in _paths:
            _basename = os.path.basename(_path)
            _name, _ = os.path.splitext(_basename)
            logging.info(_path)
            db_obj = eval(self.db_type)(_path)
            db_obj.load()
            db_obj.plot(dir_save=self.dir_plot_save, show=False, save_name_prefix=_name)
            db_obj.save_to_csv(os.path.join(self.dir_plot_save, _name + '.csv'))


class ParserV1CSV(ParserV1):
    """
    format: dir/<csvs>/<csvs>/..
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, addr_in, dir_plot_save)

    def _get_filtered_paths(self):
        csv_paths = glob.glob(os.path.join(self.dir_in, '**', '*.CSV'), recursive=True)
        return csv_paths

    def parse(self):
        _paths = self._get_filtered_paths()
        logging.info(_paths)
        for _path in _paths:
            _basename = os.path.basename(_path)
            _name, _ = os.path.splitext(_basename)
            logging.info(_path)
            db_obj = eval(self.db_type)(_path)
            db_obj.load()
            db_obj.plot(dir_save=self.dir_plot_save, show=False, save_name_prefix=_name)


class ParserV2CSV(ParserBase):
    """
    format: dir/<arbitrary>/<arbitrary>/... [with CSV files]
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, dir_plot_save)
        self.dir_in = addr_in

    def _get_filtered_paths(self):
        csv_paths = glob.glob(os.path.join(self.dir_in, '**', '*.CSV'), recursive=True)
        return csv_paths

    def parse(self):
        _paths = self._get_filtered_paths()
        logging.info(_paths)
        for _path in _paths:
            _basename = os.path.basename(_path)
            _name, _ = os.path.splitext(_basename)
            logging.info(_path)
            db_obj = eval(self.db_type)(_path)
            if db_obj.load() < 0:
                continue
            _dir_plot_save = _path.replace(self.dir_in, self.dir_plot_save)
            make_dirs(_dir_plot_save, reset=False)
            db_obj.plot(dir_save=_dir_plot_save, show=False, save_name_prefix=_name)
