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
TOWNS = ("RABMiniAcademy", "RABMiniFortress", "RABMiniHaven", "RABMiniPreserve", "RABMiniWarMachineFactory")
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
    PREFIX_FILTERS = ("maps/", "ttberein/", "mapobjects/", )
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
        self._gen_stats()
        self._build_zip_list()

    def _gen_stats(self):
        with self.lock:
            self.total_prog = 0

        for folder, file_suf in RawData.DIRS.items():
            fullpath = os.path.join(self.h5_path, folder)
            if os.path.isdir(fullpath):
                self.total_prog += len(tuple(f for f in os.listdir(fullpath)
                                             if f.lower().endswith(file_suf)
                                             and PATCH_FILE_NAME.lower() not in f.lower())) + 1
            elif folder.lower() == "data":
                raise ValueError(f"\"{self.h5_path}\"中没有找到\"{folder}\"，\n请检查是否是正确的英雄无敌5安装文件夹")

    def _build_zip_list(self):
        self.manifest = {}
        self.zip_q = {}
        self.curr_prog = 0
        prev_timeit = time()
        logging.info(f"开始对\"{self.h5_path}\"的所有游戏数据文件扫描……")

        zfs = []
        zis = []
        self.zip_q = {}
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
                    zfs.append(fullname)
                    try:
                        zip_file_fp = ZipFile(fullname)
                        self.zip_q[fullname] = zip_file_fp
                        zis.append(zip_file_fp.infolist())
                    except BadZipFile:
                        logging.info(f"  {folder}中的{f}并不是有效的压缩文件")

                    with self.lock:
                        self.curr_prog += 1

        with self.lock:
            self.curr_stage = f"生成文件清单……"
            self.curr_prog += 1
        zs = sorted([(j.filename.lower(), j.filename, j.date_time, f) for i, f in zip(zis, zfs) if len(i) > 0 for j in i
                     if any(j.filename.lower().startswith(k) for k in RawData.PREFIX_FILTERS) \
                     and any(j.filename.lower().endswith(k) for k in RawData.SUFFIX_FILTERS)
                     and not j.is_dir()],
                    key=lambda x:(x[0], x[2]))
        self.manifest = {i[0]: (i[1], i[3]) for i in zs}
        logging.warning(f"游戏数据文件信息扫描完毕，发现{len(self.zip_q)}个相关文件，用时{time() - prev_timeit:.2f}秒。")

    def listdir(self, target: str, zips_to_exclude=set()):
        target = target.lower()
        if not target.endswith("/"):
            target = target + "/"
        if target.startswith("/"):
            target = target[1:]

        all_dirs = set()
        all_files = {}
        for filename, (true_name, zip_name) in self.manifest.items():
            if zip_name[-4:].lower() in zips_to_exclude:
                continue
            if filename.startswith(target):
                remaining = filename[len(target):]
                if "/" not in remaining:
                    all_files[filename] = (true_name, zip_name) 
                else:
                    all_dirs.add(target + remaining.split("/")[0])

        return sorted(list(all_dirs)), sorted(list(all_files.values()))


    def get_file(self, target: str):
        try:
            return self.zip_q[self.get_zipname(target)].read(target)
        except:
            return None

    def get_zipname(self, target: str):
        try:
            return self.manifest[target.lower()][1]
        except:
            return None

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
        self.total_prog = 2
        self.curr_stage = None
        self.lock = Lock()
        self.work_done = False
        self.spell_xdbs = None


    def preload(self, data:RawData):
        self._preload_maps(data)
        self._preload_heroes(data)

        jobs = ("TTBereinAllHeroes.chk", "TTBereinAllSpellsArtefacts.chk", "TTBereinRacialAbilityBoost.chk")

        self._mods_status = ModsStatusClass(*(data.get_file("TTBerein/" + j) for j in jobs))
        try:
            self._hero_rab_compat_status = self._mods_status.all_heroes + self._mods_status.racial_ability_boost
        except TypeError:
            self._hero_rab_compat_status = None

        for i in range(len(self._mods_status)):
            if self._mods_status[i] is not None:
                zip_name = data.get_zipname("TTBerein/" + jobs[i])
                logging.warning(f"发现“{os.path.basename(zip_name)}”已安装，"
                                f"可以进行“{ModsStatusNames[i]}”方面的兼容")
            else:
                logging.warning(f"没有发现{ModsStatusNames[i]}。")

        return self

    def _preload_maps(self, data: RawData):
        def _get_map_xdbs(map_dir, map_excl_set):
            result = {}
            folders, _ = data.listdir(map_dir, map_excl_set)
            for folder in folders:
                _, files = data.listdir(folder, map_excl_set)
                map_xdb_name = None
                for file_name, _ in files:
                    if os.path.basename(file_name.lower()) == "map-tag.xdb":
                        et = ET.fromstring(data.get_file(file_name))
                        map_xdb_name = os.path.dirname(file_name) + "/" + \
                            et.find("AdvMapDesc").attrib["href"].split("#")[0]
                        break
                if map_xdb_name is None:
                    continue
                result[map_xdb_name] = data.get_file(map_xdb_name)
            return result

        with self.lock:
            self.curr_stage = "正在预加载地图相关XDB文件入内存……"

        prev_timeit = time()
        self.map_xdbs = {}
        xdb_jobs = (("scenario", "maps/scenario", set([".h5m"])),
                    ("singlemissions", "maps/singlemissions", set([".h5m"])),
                    ("multiplayer", "maps/multiplayer", set([".h5m"])),
                    ("customized", "maps/multiplayer", set([".h5u", ".pak"])),
                    ("customized", "maps/rmg", set([".h5u", ".pak"])))
        for map_cat, map_dir, map_excl_set in xdb_jobs:
            if map_cat not in self.map_xdbs:
                self.map_xdbs[map_cat] = {}
            temp_dict = _get_map_xdbs(map_dir, map_excl_set)
            self.map_xdbs[map_cat] = {**self.map_xdbs[map_cat], **temp_dict}

        with self.lock:
            self.curr_prog += 1

        logging.warning(f"地图数据预加载完毕，发现{len(self.map_xdbs)}个相关文件，用时{time() - prev_timeit:.2f}秒。")

    def _preload_heroes(self, data: RawData):
        def _get_hero_xdbs(map_dir):
            result = {}
            folders, _ = data.listdir(map_dir)
            for folder in folders:
                _, files = data.listdir(folder)
                for file_name, _ in files:
                    if os.path.basename(file_name.lower()).endswith(".xdb"):
                        xdb_content = data.get_file(file_name)
                        if b"<AdvMapHeroShared" in xdb_content:
                            et = ET.fromstring(xdb_content)
                            if et.tag == "AdvMapHeroShared":
                                result[file_name] = et

            return result

        with self.lock:
            self.curr_prog += 1
            self.curr_stage = "正在预加载英雄相关XDB文件入内存……"

        prev_timeit = time()
        self.hero_xdbs = _get_hero_xdbs("MapObjects/")
        logging.warning(f"英雄数据预加载完毕，发现{len(self.hero_xdbs)}个相关文件，用时{time() - prev_timeit:.2f}秒。")

    def work(self, map_options: dict[str, ModsStatusClass[bool]], hero_options: dict[str, bool]):
        with self.lock:
            self.curr_prog = 1
            self.work_done = False

        if all(j is False for i in map_options.values() for j in i):
            raise ValueError("无任何选项被勾选，退回！")

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

        num_map_xmls = sum(len(v) for k, v in self.map_xdbs.items() if any(i for i in map_options[k]))
        num_hero_xmls = 0 if all(i.racial_ability_boost is False for i in map_options.values()) else len(self.hero_xdbs)
        with self.lock:
            self.total_prog = num_map_xmls +  1 if num_hero_xmls else 0

        try:
            merged_patch, _ = remove_merged_patch()
        except PermissionError as e:
            raise ValueError(str(e))

        try:
            with ZipFile(merged_patch, "w", compression=ZIP_DEFLATED,
                        compresslevel=9) as zfp:
                logging.info("开始生成兼容文件")
                if num_map_xmls > 0:
                    logging.info(f"  共有{num_map_xmls}个地图xdb文件需要处理")
                    self._work_maps(map_options, zfp)
                if num_hero_xmls > 0:
                    logging.info(f"  共有{num_hero_xmls}个英雄xdb文件需要处理")
                    self._work_heroes(hero_options, zfp)
                logging.warning(f"兼容补丁文件{merged_patch}已经生成")

        except PermissionError:
            err_msg = f"无法创建{merged_patch}。请检查你是否对该文件夹有写权限。"
            logging.warning("出错，任务中断！"+ err_msg)
            raise ValueError()

        with self.lock:
            self.work_done = True

        return self

    def _work_maps(self, map_options: dict[str, ModsStatusClass[bool]], zfp: ZipFile):
        rab_xdbs = {i: ET.fromstring(per.get_xml(i + ".xml")) for i in TOWNS}
        all_artefacts_set = set(i.text for i in ET.fromstring(per.get_xml("AllArtefactsNoAdventure.xml")))
        all_spells_set = set(i.text for i in ET.fromstring(per.get_xml("AllSpellsNoAdventure.xml")))
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

        for cat in self.map_xdbs:
            if any(i for i in map_options[cat]):
                for xml_name in self.map_xdbs[cat]:
                    sub_prev_timeit = time()
                    with self.lock:
                        self.curr_stage = f"正在处理地图文件{xml_name}"
                        self.curr_prog += 1

                    if type(self.map_xdbs[cat][xml_name]) is not ET.Element:
                        self.map_xdbs[cat][xml_name] = ET.fromstring(self.map_xdbs[cat][xml_name])

                    if map_options[cat].all_heroes is True:
                        _enable_all_heroes(self.map_xdbs[cat][xml_name])
                    if map_options[cat].all_spells_artefacts is True:
                        _enable_all_spells_artefacts(self.map_xdbs[cat][xml_name], cat)
                    if map_options[cat].racial_ability_boost is True:
                        _add_missing_towns(self.map_xdbs[cat][xml_name], rab_xdbs)

                    ET.indent(self.map_xdbs[cat][xml_name], space="    ", level=0)
                    zfp.writestr(xml_name, ET.tostring(self.map_xdbs[cat][xml_name], short_empty_elements=True,
                                                        encoding='utf8', method='xml'))

                    with self.lock:
                        if self.work_done is True:
                            logging.warning("用户中断了操作！")
                            raise InterruptedError
                    logging.info(f"    地图文件{xml_name}处理完毕，耗时{time() - sub_prev_timeit:.2f}秒；")

        logging.warning(f"  地图xdb文件处理完毕，共耗时{time() - prev_timeit:.2f}秒。")

        return self

    def _work_heroes(self, hero_options: dict[str, ModsStatusClass[bool]], zfp: ZipFile):
        def _load_spell_xdb(hero_class):
            xml_name = "spells_{}.xml".format(hero_class[len("HERO_CLASS_"):])
            try:
                spell_et = ET.fromstring(per.get_xml(xml_name))
            except FileNotFoundError:
                return set()
            return {i.text for i in spell_et.findall("Item")}

        def __union_items_btw_et_and_set(et1: ET.Element, set2: set[str]):
            # et1 will be modified
            set1 = set(i.text for i in et1)
            for i in set2:
                if i not in set1:
                    ele = ET.Element("Item")
                    ele.text = i
                    et1.append(ele)

        if self.spell_xdbs is None:
            self.spell_xdbs = {}

        with self.lock:
            self.curr_stage = f"正在处理英雄文件数据文件"

        prev_timeit = time()

        for hero_xml, hero_et in self.hero_xdbs.items():

            hero_class = hero_et.find("Class").text
            if hero_class == "HERO_CLASS_RUNEMAGE":
                pass
            if hero_class not in self.spell_xdbs:
                self.spell_xdbs[hero_class] = _load_spell_xdb(hero_class)
            __union_items_btw_et_and_set(hero_et.find("Editable").find("spellIDs"), self.spell_xdbs[hero_class])

            ET.indent(hero_et, space="    ", level = 0)
            zfp.writestr(hero_xml, ET.tostring(hero_et, short_empty_elements=True, encoding='utf8', method='xml'))

            with self.lock:
                if self.work_done is True:
                    logging.warning("用户中断了操作！")
                    raise InterruptedError
            logging.info(f"    英雄文件{hero_xml}处理完毕；")

        logging.warning(f"  英雄xdb文件处理完毕，共耗时{time() - prev_timeit:.2f}秒。")

        return self


    def cancel(self):
        with self.lock:
            self.work_done = True

    @property
    def mod_status(self):
        return self._mods_status

    @property
    def hero_rab_compat_status(self):
        return self._hero_rab_compat_status

    @staticmethod
    def get_time_weightage():
        return 0.25

    def get_progress(self):
        with self.lock:
            return self.curr_prog / self.total_prog

    def get_stage(self):
        with self.lock:
            return self.curr_stage
