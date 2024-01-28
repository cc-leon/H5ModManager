import os
import sys

class Persistence:
    FILE_NAME = "H5SkillPredict.ini"

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
        self._get_resource_path()
        pass

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
        return open(os.path.join(self.rc_path, xml_name), "r").read()

    def _get_resource_path(self):
        try:
            self.rc_path = sys._MEIPASS
        except Exception:
            self.rc_path = os.path.abspath(".")

per = Persistence()
