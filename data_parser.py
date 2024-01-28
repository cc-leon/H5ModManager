import logging
import os
import xml.etree.ElementTree as ET
from time import time
from zipfile import BadZipFile, ZipFile
from threading import Lock


# Global and game info
info = None


class RawData:
    DIRS = {"data": ".pak", "UserMods": ".h5u", "Maps": ".h5m"}
    PREFIX_FILTERS = ("maps/", )
    SUFFIX_FILTERS = ("map.xdb", )

    def __init__(self, h5_path: str):
        self.h5_path = h5_path
        self.zip_q = None
        self.manifest = None
        self.tree = None
        self.curr_stage = "估计中"
        self.curr_prog = 0
        self.total_prog = 1
        self.lock = Lock()

    def run(self):
        prev_timeit = time()
        logging.info(f"开始对\"{self.h5_path}\"的所有游戏数据文件扫描……")
        self._gen_stats()
        self._build_zip_list()
        logging.warning(f"游戏数据文件信息扫描完毕，发现{len(self.zip_q)}个相关文件，用时{time() - prev_timeit:.2f}秒。")
        prev_timeit = time()

    def _gen_stats(self):
        with self.lock:
            self.total_prog = 0

            for folder, file_suf in RawData.DIRS.items():
                fullpath = os.path.join(self.h5_path, folder)
                if os.path.isdir(fullpath):
                    self.total_prog += len(tuple(f for f in os.listdir(fullpath)
                                                 if f.lower().endswith(file_suf)))
                else:
                    raise ValueError(f"\"{self.h5_path}\"中没有找到\"{folder}\"")

    def _build_zip_list(self):
        self.manifest = {}
        self.zip_q = {}
        self.curr_prog = 0

        for folder, file_suf in RawData.DIRS.items():
            fullpath = os.path.join(self.h5_path, folder)
            if not os.path.isdir(fullpath):
                continue
            with self.lock:
                self.curr_stage = f"正在扫描\"{folder}\"文件夹"

            for f in os.listdir(fullpath):
                fullname = os.path.join(fullpath, f)
                if os.path.isfile(fullname) and fullname.lower().endswith(file_suf):
                    try:
                        zip_file = ZipFile(fullname)
                        with self.lock:
                            self.curr_prog += 1
                        logging.info(f"  在\"{folder}\"发现{f}")

                        infolist = zip_file.infolist()

                        self.manifest[fullname] = \
                            dict(zip([i.filename.lower() for i in infolist
                                      if any(i.filename.lower().startswith(j) for j in RawData.PREFIX_FILTERS) \
                                        and any(i.filename.lower().endswith(j) for j in RawData.SUFFIX_FILTERS)
                                        and not i.is_dir()],
                                     [(k.filename, k.date_time) for k in infolist
                                       if any(k.filename.lower().startswith(l) for l in RawData.PREFIX_FILTERS) \
                                        and any(k.filename.lower().endswith(l) for l in RawData.SUFFIX_FILTERS) \
                                        and not k.is_dir()]))

                        if len(self.manifest[fullname]) > 0:
                            self.zip_q[fullname] = zip_file
                            logging.info(f"    压缩包中有{len(self.manifest[fullname])}个相关文件；")

                    except BadZipFile:
                        logging.info(f"  {folder}中的{f}并不是有效的压缩文件")

    def walk(self, target: str):
        raise NotImplementedError

    def listdir(self, target: str, zips_to_exclude=set()):
        target = target.lower()
        if not target.endswith("/"):
            target = target + "/"
        if target.startswith("/"):
            target = target[1:]

        all_dirs = set()
        all_files = {}
        for zip_name, manifest in self.manifest.items():
            if zip_name[-4:].lower() in zips_to_exclude:
                continue
            for f, fi in manifest.items():
                if f.startswith(target):
                    remaining = f[len(target):]
                    if "/" not in remaining:
                        if f not in all_files:
                            all_files[f] = (fi[1], zip_name, fi[0]) 
                        else:
                            if all_files[f][0] <= fi[1]:
                                all_files[f] = (fi[1], zip_name, fi[0])
                    else:
                        all_dirs.add(target + remaining.split("/")[0])

        return sorted(list(all_dirs)), {v[2]: v[1] for k, v in all_files.items()}

    def get_file_by_zip(self, target: str, zip_name: str):
        return self.zip_q[zip_name].read(self.manifest[zip_name][target][0])

    def get_file(self, target: str):
        pass

    def get_progress(self):
        with self.lock:
            return self.curr_prog / self.total_prog

    def get_stage(self):
        with self.lock:
            return self.curr_stage

    @staticmethod
    def get_time_weightage():
        return 1.0


class GameInfo:

    def __init__(self):
        self.curr_prog = 0
        self.total_prog = 1567
        self.curr_stage = None
        self.lock = Lock()

    def run(self, data: RawData):
        # Get all scenarios map.xdb
        scenarios = []
        folders, _ = data.listdir("maps/scenario", set([".h5m"]))
        for folder in folders:
            _, files = data.listdir(folder, set([".h5m"]))
            for file in files.items():
                scenarios.append(file)

        # Get all singlemissions map.xdb
        singlemissions = []
        folders, _ = data.listdir("maps/singlemissions", set([".h5m"]))
        for folder in folders:
            _, files = data.listdir(folder, set([".h5m"]))
            for file in files.items():
                singlemissions.append(file)

        # Get all multiplayler map.xdb
        multiplayer = []
        folders, _ = data.listdir("maps/multiplayer", set([".h5m"]))
        for folder in folders:
            _, files = data.listdir(folder, set([".h5m"]))
            for file in files.items():
                multiplayer.append(file)

        # Get all customized map.xdb
        customized = []
        folders, _ = data.listdir("maps/multiplayer", set([".h5u", ".pak"]))
        for folder in folders:
            _, files = data.listdir(folder, set([".h5u", ".pak"]))
            for file in files.items():
                customized.append(file)
        folders, _ = data.listdir("maps/rmg", set([".h5u", ".pak"]))
        for folder in folders:
            _, files = data.listdir(folder, set([".h5u", ".pak"]))
            for file in files.items():
                customized.append(file)

        return self

    @staticmethod
    def get_time_weightage():
        return 0.0

    def get_progress(self):
        with self.lock:
            return self.curr_prog / self.total_prog

    def get_stage(self):
        with self.lock:
            return self.curr_stage
