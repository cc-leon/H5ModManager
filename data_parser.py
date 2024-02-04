import logging
import os
import xml.etree.ElementTree as ET
from collections import namedtuple
from time import time
from zipfile import BadZipFile, ZipFile, ZIP_DEFLATED
from threading import Lock

from persistence import per


# Global and game info
info = None
ModsStatusClass = namedtuple("ModsStatusClass", ["all_heroes", "all_spells_artefacts", "racial_ability_boost"])
ModsStatusNames = ("全英雄Mod", "全魔法全宝物Mod", "种族能力增强Mod")
TOWNS = ("RABMiniAcademy", "RABMiniFortress", "RABMiniHaven", "RABMiniPreserve")
PATCH_FILE_NAME = "TTBereinMergedPatch.h5u"


def remove_merged_patch():
    merged_patch = os.path.join(per.last_path, "UserMODs", PATCH_FILE_NAME)
    if os.path.isfile(merged_patch):
        try:
            os.remove(merged_patch)
            return merged_patch, True
        except PermissionError:
            err_msg = f"无法移除{merged_patch}。\n请检查游戏或者地图编辑器是否正在运行，如果是的话请关闭游戏或者地图编辑器。"
            raise PermissionError(err_msg)

    return merged_patch, False

class RawData:
    DIRS = {"data": ".pak", "UserMods": ".h5u", "Maps": ".h5m"}
    PREFIX_FILTERS = ("maps/", "ttberein/", )
    SUFFIX_FILTERS = (".xdb", ".chk", )

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
                                             if f.lower().endswith(file_suf)
                                             and PATCH_FILE_NAME.lower() not in f.lower()))
            elif folder.lower() == "data":
                raise ValueError(f"\"{self.h5_path}\"中没有找到\"{folder}\"，\n请检查是否是正确的英雄无敌5安装文件夹")

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
                if os.path.isfile(fullname) and fullname.lower().endswith(file_suf) and \
                    PATCH_FILE_NAME.lower() not in f.lower():
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

    def walk(self, target: str):
        pass

    def get_file_by_zip(self, target: str, zip_name: str):
        return self.zip_q[zip_name].read(self.manifest[zip_name][target.lower()][0])

    def get_file(self, target: str):
        target = target.lower()
        result = (None, None, None)
        for zip_name in self.zip_q:
            for fz_info in self.zip_q[zip_name].infolist():
                if fz_info.filename.lower() == target:
                    if result[0] is None or fz_info.date_time >= result[0]:
                        result = (fz_info.date_time, fz_info.filename, zip_name)

        return result[1:]

    def get_progress(self):
        with self.lock:
            return self.curr_prog / self.total_prog

    def get_stage(self):
        with self.lock:
            return self.curr_stage

    @staticmethod
    def get_time_weightage():
        return 2.80


class GameInfo:
    def __init__(self):
        self.curr_prog = 0
        self.total_prog = 1e5
        self.curr_stage = None
        self.lock = Lock()
        self.work_done = False

    def preload(self, data: RawData):
        def _get_map_xdbs(map_dir, map_excl_set):
            result = {}
            folders, _ = data.listdir(map_dir, map_excl_set)
            for folder in folders:
                _, files = data.listdir(folder, map_excl_set)

                map_xdb_name = None
                for file_name, zip_name in files.items():
                    if os.path.basename(file_name.lower()) == "map-tag.xdb":
                        et = ET.fromstring(data.get_file_by_zip(file_name, zip_name))
                        map_xdb_name = os.path.dirname(file_name) + "/" + \
                            et.find("AdvMapDesc").attrib["href"].split("#")[0]
                        break

                if map_xdb_name is None:
                    continue

                for file_name, zip_name in files.items():
                    if map_xdb_name.lower() == file_name.lower():
                        result[file_name] = data.get_file_by_zip(file_name, zip_name)
                        break

            return result

        with self.lock:
            self.curr_prog = 0
            self.curr_stage = "正在预加载XDB文件入内存……"

        prev_timeit = time()
        num_files = 0
        self.xdbs = {}
        xdb_jobs = (("scenario", "maps/scenario", set([".h5m"])),
                    ("singlemissions", "maps/singlemissions", set([".h5m"])),
                    ("multiplayer", "maps/multiplayer", set([".h5m"])),
                    ("customized", "maps/multiplayer", set([".h5u", ".pak"])),
                    ("customized", "maps/rmg", set([".h5u", ".pak"])))
        for map_cat, map_dir, map_excl_set in xdb_jobs:
            if map_cat not in self.xdbs:
                self.xdbs[map_cat] = {}
            temp_dict = _get_map_xdbs(map_dir, map_excl_set)
            self.xdbs[map_cat] = {**self.xdbs[map_cat], **temp_dict}
            num_files += len(temp_dict)

        with self.lock:
            self.curr_prog = 1

        logging.warning(f"游戏数据预加载完毕，发现{num_files}个相关文件，用时{time() - prev_timeit:.2f}秒。")

        jobs = ("TTBereinAllHeroes.chk", "TTBereinAllSpellsArtefacts.chk", "TTBereinRacialAbilityBoost.chk")

        self._mods_status = ModsStatusClass(*(data.get_file("TTBerein/" + j)[1] for j in jobs))
        for i in range(len(self._mods_status)):
            if self._mods_status[i] is not None:
                logging.warning(f"发现“{os.path.basename(self._mods_status[i])}”已安装，"
                                f"可以进行“{ModsStatusNames[i]}”方面的兼容")
            else:
                logging.warning(f"没有发现{ModsStatusNames[i]}。")

        return self

    def work(self, cb_matrix: dict[str, ModsStatusClass[bool]]):
        with self.lock:
            self.curr_prog = 1
            self.work_done = False

        if all(j is False for i in cb_matrix.values() for j in i):
            raise ValueError("无任何选项被勾选，退回！")

        
        rab_xdbs = {i:ET.fromstring(per.get_xml(i + ".xml")) for i in TOWNS}
        all_artefacts_set = set(i.text for i in ET.fromstring(per.get_xml("AllArtefactsNoAdventure.xml")))
        all_spells_set = set(i.text for i in ET.fromstring(per.get_xml("AllSpellsNoAdventure.xml")))

        num_xmls = sum(len(v) for k, v in self.xdbs.items() if any(i for i in cb_matrix[k]))
        with self.lock:
            self.total_prog = num_xmls
        logging.info("开始生成兼容文件")
        logging.info(f"  共有{num_xmls}个现有xdb文件需要处理")
        prev_timeit = time()

        def __empty_element_by_tag(et: ET.Element, tag_to_empty):
            to_remove_et = et.find(tag_to_empty)
            to_remove_et_i = list(et).index(to_remove_et)
            et.remove(to_remove_et)
            et.insert(to_remove_et_i, ET.Element(tag_to_empty))

        def __union_items_btw_et_and_set(et1: ET.Element, set2: set[str]):
            # et1 will be modified
            set1 = set(i.text for i in et1)
            if len(set1) > 0:
                for i in set2:
                    if i not in set1:
                        ele = ET.Element("Item")
                        ele.text = i
                        et1.append(ele)

        def _add_missing_towns(map_et: ET.Element, rabs: dict[str, ET.Element]):
            towns = set()
            et = map_et.find("objects")
            for i in et.findall("Item"):
                adv_town_et = i.find("AdvMapTown")
                if adv_town_et is not None:
                    towns.add(adv_town_et.find("Name").text)

            for rab in rabs:
                if rab not in towns:
                    map_et.find("objects").append(rabs[rab])

        def _enable_all_spells_artefacts(map_et: ET.Element, cat: str):
            def _sub_process(tag, all_set):
                if not (cat == "scenario" and tag == "artifactIDs"):
                    if cat in ("scenario", "singlemissions"):
                        __union_items_btw_et_and_set(map_et.find(tag), all_set)
                    else:
                        __empty_element_by_tag(map_et, tag)


            params = (("spellIDs", all_spells_set), ("artifactIDs", all_artefacts_set))
            for param1, param2 in params:
                _sub_process(param1, param2)


        def _enable_all_heroes(map_et: ET.Element):
            __empty_element_by_tag(map_et, "AvailableHeroes")

        # All heroes mod
        mod_dir = os.path.join(per.last_path, "UserMODs")
        if not os.path.isdir(mod_dir):
            try:
                os.makedirs(mod_dir)
            except FileExistsError:
                err_msg = f"{mod_dir}是个文件，不是文件夹，请删除该文件后再运行。"
                logging.warning("出错，任务中断！" + err_msg)
                raise ValueError(err_msg)
            except OSError:
                err_msg = f"无法创建{mod_dir}，请检查游戏文件夹是否是只读。"
                logging.warning("出错，任务中断！"+ err_msg)
                raise ValueError(err_msg)

        try:
            merged_patch, _ = remove_merged_patch()
        except PermissionError as e:
            raise ValueError(str(e))

        try:
            with ZipFile(merged_patch, "w", compression=ZIP_DEFLATED,
                        compresslevel=9) as zfp:
                for cat in self.xdbs:
                    if any(i for i in cb_matrix[cat]):
                        for xml_name in self.xdbs[cat]:
                            sub_prev_timeit = time()
                            with self.lock:
                                self.curr_stage = f"正在处理{xml_name}"
                                self.curr_prog += 1

                            if type(self.xdbs[cat][xml_name]) is not ET.Element:
                                self.xdbs[cat][xml_name] = ET.fromstring(self.xdbs[cat][xml_name])

                            if cb_matrix[cat].all_heroes is True:
                                _enable_all_heroes(self.xdbs[cat][xml_name])
                            if cb_matrix[cat].all_spells_artefacts is True:
                                _enable_all_spells_artefacts(self.xdbs[cat][xml_name], cat)
                            if cb_matrix[cat].racial_ability_boost is True:
                                _add_missing_towns(self.xdbs[cat][xml_name], rab_xdbs)

                            ET.indent(self.xdbs[cat][xml_name], space="    ", level=0)
                            zfp.writestr(xml_name, ET.tostring(self.xdbs[cat][xml_name], short_empty_elements=True,
                                                                encoding='utf8', method='xml'))

                            with self.lock:
                                if self.work_done is True:
                                    logging.warning("用户中断了操作！")
                                    raise InterruptedError
                            logging.info(f"  {xml_name}处理完毕，耗时{time() - sub_prev_timeit:.2f}秒；")
        except PermissionError:
            err_msg = f"无法创建{merged_patch}。请检查你是否对该文件夹有写权限。"
            logging.warning("出错，任务中断！"+ err_msg)
            raise ValueError()


        logging.warning(f"现有xdb文件处理完毕，生成文件{merged_patch}，共耗时{time() - prev_timeit:.2f}秒。")
        prev_timeit = time()

        with self.lock:
            self.work_done = True

        return self

    def cancel(self):
        with self.lock:
            self.work_done = True

    @property
    def mod_status(self):
        return self._mods_status

    @staticmethod
    def get_time_weightage():
        return 0.25

    def get_progress(self):
        with self.lock:
            return self.curr_prog / self.total_prog

    def get_stage(self):
        with self.lock:
            return self.curr_stage
