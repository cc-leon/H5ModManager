import os
import sys
import xml.etree.ElementTree as ET
import uuid
import itertools
from copy import deepcopy

class Persistence:
    FILE_NAME = "H5SkillPredict.ini"
    VERSION = "0.50"
    TOWNS = ("RABMiniAcademy", "RABMiniFortress", "RABMiniHaven", "RABMiniPreserve", "RABMiniStronghold", 
             "RABMiniWarMachineFactory")

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
        self._rab_xdbs = {}
        self._all_artefacts_set = set()
        self._all_spells_set = set()
        self._perk_swaps = {}
        self._specialization_swaps = {}
        self._artificer_artefact_skeleton = None
        self._artificer_artefact_names = {}

        self._get_resource_path()
        self._load_towns_spells_artifacts()
        self._load_swaps()
        self._load_artificer_artefacts()

    def save(self):
        contents = (self.last_path, self.show_log,
                    f"{self.main_x},{self.main_y}",
                    f"{self.log_x},{self.log_y}")

        with open(Persistence.FILE_NAME, 'w') as fp:
            for i in contents:
                fp.write(f"{i}\n")

    def get_about_txt(self):
        return open(self._get_file("About.txt")).read()

    def get_xml(self, xml_name):
        return open(self._get_file(xml_name)).read()

    def get_7za(self):
        return self._get_file("7za.exe")

    def get_ico(self):
        return self._get_file("Angel.ico")

    def get_artificer_artefact_xdb(self, name):
        spell_id, artefact_href = self._artificer_artefact_names[name]
        new_uuid = "item_{}".format(str(uuid.uuid4()).upper())
        result = deepcopy(self._artificer_artefact_skeleton)
        result.attrib["id"] = new_uuid
        adv_arti_et = result.find("AdvMapArtifact")
        adv_arti_et.find("Name").text = name
        adv_arti_et.find("Shared").attrib["href"] = artefact_href
        adv_arti_et.find("spellID").text = spell_id
        return result

    def _load_towns_spells_artifacts(self):
        self._rab_xdbs = {i: ET.fromstring(self.get_xml(i + ".xml")) for i in Persistence.TOWNS}
        self._all_artefacts_set = set(i.text for i in ET.fromstring(self.get_xml("AllArtefactsNoAdventure.xml")))
        self._all_spells_set = set(i.text for i in ET.fromstring(self.get_xml("AllSpellsNoAdventure.xml")))

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

    def _load_artificer_artefacts(self):
        self._artificer_artefact_names = {}
        arti_xdb = ET.fromstring(open(self._get_file("RABArtificerArtefacts.xml")).read())
        artificer_artefact_types = {i.tag: i.attrib["href"] for i in arti_xdb.find("Types")}
        spells = tuple(i.text for i in arti_xdb.find("Spells"))
        indices = tuple(range(1, int(arti_xdb.find("Counts").text) + 1))
        self._artificer_artefact_skeleton = arti_xdb.find("Skeleton").find("Item")

        for arti, sp, i in itertools.product(artificer_artefact_types, spells, indices):
            self._artificer_artefact_names["{}_{}_{}".format(arti, sp, i)] = (sp, artificer_artefact_types[arti])

    def _get_resource_path(self):
        try:
            self.rc_path = sys._MEIPASS
        except Exception:
            self.rc_path = os.path.abspath(".")

    def _get_file(self, file_name):
        result = os.path.join(self.rc_path, file_name)
        if os.path.isfile(result):
            return result
        else:
            raise FileNotFoundError(result)

    @property
    def rab_xdbs(self):
        return self._rab_xdbs

    @property
    def all_artefacts_set(self):
        return self._all_artefacts_set

    property
    def all_spells_set(self):
        return self._all_spells_set

    @property
    def perk_swaps(self):
        return self._perk_swaps

    @property
    def specialization_swaps(self):
        return self._specialization_swaps

    @property
    def artificer_artefact_names(self):
        return self._artificer_artefact_names

per = Persistence()
