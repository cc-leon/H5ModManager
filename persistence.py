import os
import sys
import xml.etree.ElementTree as ET

class Persistence:
    FILE_NAME = "H5SkillPredict.ini"
    VERSION = "0.90"

    def __init__(self):
        if os.path.isfile(Persistence.FILE_NAME):
            with open(Persistence.FILE_NAME) as fp:
                contents = tuple(line.rstrip() for line in fp)
        else:
            contents = ("", "True", "300,10", "1150,10")

        self.last_path = contents[0]
        self.show_log = True if contents[1] == "True" else False
        self.main_x, self.main_y = [int(i) for i in contents[2].split(",")]
        self.log_x, self.log_y = [int(i) for i in contents[3].split(",")]
        self.rc_path = ""
        self._perk_swaps = {}
        self._specialization_swaps = {}

        self._get_resource_path()
        self._load_swaps()

    def save(self):
        contents = (self.last_path, self.show_log,
                    f"{self.main_x},{self.main_y}",
                    f"{self.log_x},{self.log_y}")

        with open(Persistence.FILE_NAME, 'w') as fp:
            for i in contents:
                fp.write(f"{i}\n")

    def get_about_txt(self):
        return open(os.path.join(self.rc_path, "About.txt"), "r").read()

    def get_xml(self, xml_name):
        fullpath = os.path.join(self.rc_path, xml_name)
        if os.path.isfile(fullpath):
            return open(fullpath, "r").read()
        else:
            raise FileNotFoundError(fullpath)

    def get_7za(self):
        result = os.path.join(self.rc_path, "7za.exe")
        if os.path.isfile(result):
            return result
        else:
            raise FileNotFoundError(result)

    def get_ico(self):
        result = os.path.join(self.rc_path, "Angel.ico")
        if os.path.isfile(result):
            return result
        else:
            raise FileNotFoundError(result)

    def _load_swaps(self):
        xml_text = self.get_xml("RABSwaps.xml")
        root = ET.fromstring(xml_text)
        perks_et = root.find("Perks")
        specs_et = root.find("Specializations")

        for i in perks_et:
            self._perk_swaps[i.find("Perk1").text] = (i.find("Perk2").text, i.find("Skill1").text)

        for i in specs_et:
            self._specialization_swaps[i.find("Specialization1").text] = \
                (i.find("Specialization2").text, i.find("SpecializationNameFileRef").attrib["href"], 
                 i.find("SpecializationDescFileRef").attrib["href"], i.find("SpecializationIcon").attrib["href"])

    def _get_resource_path(self):
        try:
            self.rc_path = sys._MEIPASS
        except Exception:
            self.rc_path = os.path.abspath(".")

    @property
    def perk_swaps(self):
        return self._perk_swaps

    @property
    def specialization_swaps(self):
        return self._specialization_swaps

per = Persistence()
