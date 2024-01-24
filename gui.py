import logging
import random
from functools import partial
from pprint import pprint
from threading import Thread, Lock
from queue import Queue, Empty
from copy import deepcopy
from tkinter import END
from tkinter import font, messagebox, filedialog, Tk, Menu, scrolledtext, Toplevel, Canvas
from tkinter.ttk import Frame, Label, Button, Progressbar

from PIL import ImageTk, Image, ImageOps

from data_parser import RawData, GameInfo
from calculator import Hero
from persistence import per
import data_parser as gg


TITLE = "英雄无敌5技能概率计算器"


def _rgb(*rgb): return "#%02x%02x%02x" % rgb


def _proc_xxx_ico(img, x=64, y=64):
    result = img.copy()
    result.thumbnail((x, y), Image.Resampling.LANCZOS)
    return ImageTk.PhotoImage(result)


def _show_msg(func, msg, event=None):
    func(TITLE, msg)


class InteractiveCanvas(Canvas):

    class UIToolTip():
        WAIT_TIME = 100
        WRAP_LENGTH = 180

        def __init__(self, canvas, widget_name, name, description):
            self.canvas = canvas
            self.name = name
            self.description = description
            self.widget_name = widget_name
            canvas.tag_bind(widget_name, "<Button-3>", self.enter)
            canvas.tag_bind(widget_name, "<Leave>", self.leave)
            canvas.tag_bind(widget_name, "<ButtonPress>", self.leave)
            self.id = None
            self.tw = None

        def unload(self):
            try:
                self.canvas.tag_unbind(self.widget_name, "<Button-3>")
                self.canvas.tag_unbind(self.widget_name, "<Leave>")
                self.canvas.tag_unbind(self.widget_name, "<ButtonPress>")
            except Exception:
                pass

        def enter(self, event=None): self.schedule(event)

        def leave(self, event=None):
            self.unschedule()
            self.hidetip()

        def schedule(self, event=None):
            self.unschedule()
            self.id = self.canvas.after(InteractiveCanvas.UIToolTip.WAIT_TIME, self.showtip, event)

        def unschedule(self):
            id = self.id
            self.id = None
            if id:
                self.canvas.after_cancel(id)

        def showtip(self, event=None):
            x, y = event.x_root, event.y_root
            x += 10
            y += 10
            self.tw = Toplevel(self.canvas)
            self.tw.wm_overrideredirect(True)
            self.tw.wm_geometry("+%d+%d" % (x, y))

            label = Label(self.tw, text=self.name, anchor="center",
                          background="#ffffff", relief="solid", borderwidth=1,
                          font=(per.font, 12, "bold"))
            label.grid(column=0, row=0, sticky="ew")

            label = Label(self.tw, text=self.description, anchor="w",
                          background="#ffffff", relief="solid", borderwidth=1,
                          font=(per.font, 12, ),
                          wraplength=InteractiveCanvas.UIToolTip.WRAP_LENGTH)
            label.grid(column=0, row=1, sticky="ew")

        def hidetip(self):
            tw = self.tw
            self.tw = None
            if tw:
                tw.destroy()

    class HeroUI():
        def __init__(self, offset, canvas, src):
            self.canvas = canvas
            self.offset = offset
            self.tk_images = None
            self.solid_ico = None
            self.empty_ico = None
            self.brother = None
            self.src = src
            self.ele = []
            self.tips = []

        def unload(self):
            for i in self.tips:
                i.unload()

            for i in self.ele:
                self.canvas.tag_unbind(i, "<Button-1>")
                self.canvas.delete(i)

        @property
        def hero(self): return self._hero

        @hero.setter
        def hero(self, new_hero):
            self.unload()
            self._hero = new_hero

            offset = self.offset
            canvas = self.canvas
            hero_info = gg.info.hero_info[new_hero.id]
            hero = new_hero
            heroes_info = gg.info.hero_info
            class_info = gg.info.class_info
            class2hero = gg.info.class2hero
            skill_info = gg.info.skill_info
            perk_info = gg.info.perk_info
            popup = {}
            tk_images = []
            ele = []
            tips = []

            tk_images.append(ImageTk.PhotoImage(hero_info.face))
            ele.append(canvas.create_image(*offset.face, image=tk_images[-1], anchor="nw"))
            tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], hero_info.name,
                                                    f"点击这里选择其他的{class_info[hero.race][0]}英雄"))
            popup["hero"] = Menu(canvas, tearoff=0)
            for i in class2hero[hero.race]:
                if i != hero.id:
                    tk_images.append(_proc_xxx_ico(heroes_info[i].face))
                    popup["hero"].add_command(image=tk_images[-1], label=heroes_info[i].name, compound="left",
                                              font=(per.font, 14), command=partial(self.hero_select, i))
            canvas.tag_bind(ele[-1], "<Button-1>", partial(InteractiveCanvas.HeroUI.popup_menu, popup["hero"]))

            ele.append(canvas.create_text(*offset.name, text=hero_info.name, font=(per.font, 14, "bold"),
                                          fill="grey85", anchor="nw"))
            tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], hero_info.name,
                                                    f"点击这里选择其他的{class_info[hero.race][0]}英雄"))
            canvas.tag_bind(ele[-1], "<Button-1>", partial(InteractiveCanvas.HeroUI.popup_menu, popup["hero"]))

            temp_offset = list(offset.name)
            temp_offset[1] += 22
            ele.append(canvas.create_text(*temp_offset, text=class_info[hero.race], font=(per.font, 14, "bold"),
                                          fill="white", anchor="nw"))
            tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], class_info[hero.race],
                                                    f"点击这里选择其他的势力的英雄"))
            popup["race"] = Menu(canvas, tearoff=0)
            for i in tuple(i[0] for i in class_info.items() if i[0] != hero.race):
                popup["race"].add_command(label=class_info[i], compound="left", font=(per.font, 14),
                                          command=partial(self.hero_select, random.choice(class2hero[i])))
            canvas.tag_bind(ele[-1], "<Button-1>", partial(InteractiveCanvas.HeroUI.popup_menu, popup["race"]))

            temp_offset = list(offset.level)
            temp_offset[1] += 8
            ele.append(canvas.create_text(*temp_offset, text=f"等级：{hero.level}", font=(per.font, 14, "bold"),
                                          fill="grey85", anchor="nw"))

            self.solid_ico = _proc_xxx_ico(gg.info.ui.solid_ico)
            self.empty_ico = _proc_xxx_ico(gg.info.ui.empty_ico)

            skill_ids, perk_ids = hero.slots22dtuple()
            i = -1
            for i, (s, perks) in enumerate(tuple(zip(skill_ids, perk_ids))):
                tk_images.append(_proc_xxx_ico(skill_info[s[0]].icons[s[1] - 1]))
                ele.append(canvas.create_image(*offset.slots[i][0], image=self.solid_ico, anchor="nw"))
                ele.append(canvas.create_image(*offset.slots[i][0], image=tk_images[-1], anchor="nw"))
                canvas.tag_bind(ele[-1], "<Button-1>", partial(self._skill_select, s[0]))
                tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], skill_info[s[0]].names[s[1] - 1],
                                                        skill_info[s[0]].descs[s[1] - 1]))
                j = -1
                for j, p in enumerate(perks):
                    tk_images.append(_proc_xxx_ico(perk_info[p].icon))
                    ele.append(canvas.create_image(*offset.slots[i][j + 1], image=self.solid_ico, anchor="nw"))
                    ele.append(canvas.create_image(*offset.slots[i][j + 1], image=tk_images[-1], anchor="nw"))
                    canvas.tag_bind(ele[-1], "<Button-1>", partial(self._perk_select, s[0], p))
                    tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], perk_info[p].name, perk_info[p].desc))
                for k in range(j + 1, 3 if i != 0 else 4):
                    ele.append(canvas.create_image(*offset.slots[i][k + 1], image=self.empty_ico, anchor="nw"))
                    canvas.tag_bind(ele[-1], "<Button-1>", partial(self._perk_select, s[0], None))
                    tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], "（空子技能）", "点击这里选择一个新的子技能"))

            for j in range(i + 1, 6):
                ele.append(canvas.create_image(*offset.slots[j][0], image=self.empty_ico, anchor="nw"))
                canvas.tag_bind(ele[-1], "<Button-1>", partial(self._skill_select, None))
                tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], "（空主技能）", "点击这里选择一个新的主技能"))
                for k in range(3 if j != 0 else 4):
                    ele.append(canvas.create_image(*offset.slots[j][k + 1], image=self.empty_ico, anchor="nw"))
                    tips.append(InteractiveCanvas.UIToolTip(canvas, ele[-1], "（空子技能）", "选择主技能之后才能选择子技能"))
                    canvas.tag_bind(ele[-1], "<Button-1>",
                                    partial(_show_msg, messagebox.showwarning, "选择主技能之后才能选择子技能！"))

            self.tk_images = tk_images
            self.ele = ele
            self.tips = tips

        def hero_select(self, hero_id):
            self.hero = Hero(gg.info.hero_info[hero_id])
            if self.brother is not None:
                self.brother.hero = Hero(gg.info.hero_info[hero_id])

        def _skill_select(self, id, event):
            result = self._hero.get_select_skills(id)
            temp = []
            for i, j in result:
                if i is None:
                    temp.append((None, None, None))
                else:
                    temp.append(((i, j), gg.info.skill_info[i].icons[j - 1], gg.info.skill_info[i].names[j - 1]))
            result = temp

            popup = Menu(self.canvas, tearoff=0)
            for i, j, k in result:
                if i is None:
                    self.tk_images.append(_proc_xxx_ico(gg.info.ui.empty_ico))
                    popup.add_command(image=self.tk_images[-1], label="移除该主技能", compound="left",
                                      font=(per.font, 14), command=partial(self._on_skill_perk_menu, id, None))
                    popup.add_separator()
                else:
                    self.tk_images.append(_proc_xxx_ico(j))
                    popup.add_command(image=self.tk_images[-1], label=k, compound="left", font=(per.font, 14),
                                      command=partial(self._on_skill_perk_menu, id, i))

            InteractiveCanvas.HeroUI.popup_menu(popup, event)

        def _perk_select(self, sid, pid, event):
            result = [None, None, None, None, None]
            result[0], result[1], result[2], result[3], result[4] = self._hero.get_select_perks(sid, pid)
            popup = Menu(self.canvas, tearoff=0)

            def _add_items_to_menu(result):
                for i in result:
                    self.tk_images.append(_proc_xxx_ico(gg.info.perk_info[i].icon))
                    popup.add_command(image=self.tk_images[-1], label=gg.info.perk_info[i].name, compound="left",
                                      font=(per.font, 14), command=partial(self._on_skill_perk_menu, pid, i))

            if len(result[0]) > 0:
                self.tk_images.append(_proc_xxx_ico(gg.info.ui.empty_ico))
                popup.add_command(image=self.tk_images[-1], label="移除该子技能", compound="left",
                                  font=(per.font, 14), command=partial(self._on_skill_perk_menu, pid, None))
                if any(len(result[i]) > 0 for i in range(1, 4)):
                    popup.add_separator()
            if len(result[1]) > 0:
                _add_items_to_menu(result[1])
                if any(len(result[i]) > 0 for i in range(2, 4)):
                    popup.add_separator()
            if len(result[2]) > 0:
                _add_items_to_menu(result[2])
                if len(result[3]) > 0:
                    popup.add_separator()
            for i, j in enumerate(result[3]):
                self.tk_images.append(_proc_xxx_ico(gg.info.perk_info[j].icon.convert("LA").convert("RGBA")))
                popup.add_command(image=self.tk_images[-1], label=gg.info.perk_info[j].name, compound="left",
                                  font=(per.font, 14),
                                  command=partial(_show_msg, messagebox.showerror, "\n".join(result[4][i])))
            InteractiveCanvas.HeroUI.popup_menu(popup, event)

        def _on_skill_perk_menu(self, old_id, id):
            if old_id in gg.info.skill_info or (id is not None and id[0] in gg.info.skill_info):
                self._hero.replace_skill(old_id, id)
            else:
                self._hero.replace_perk(old_id, id)
            self.hero = self._hero
            self.hero.compromise(self.brother._hero, self.src)
            self.brother.hero = self.brother._hero

        @staticmethod
        def popup_menu(pop_menu, event):
            try:
                pop_menu.tk_popup(event.x_root, event.y_root)
            finally:
                pop_menu.grab_release()

    def __init__(self, master=None, **kw):
        super().__init__(master=master, **kw)
        self.bg_img = None
        self.bg = None
        self.ui_hero = {}

    def load_bg(self):
        if self.bg is None:
            self.config(width=gg.info.ui.bg.width, height=gg.info.ui.bg.height)
            self.bg_img = ImageTk.PhotoImage(gg.info.ui.bg)
            self.bg = self.create_image(0, 0, image=self.bg_img, anchor="nw")
            self.create_text(170, 50, text="起点状态", font=(per.font, 20, "bold"), fill=_rgb(41, 71, 97), anchor="nw")
            self.create_text(530, 50, text="终点状态", font=(per.font, 20, "bold"), fill=_rgb(41, 71, 97), anchor="nw")

    def load_ui(self, hero_id):
        for i in ("src", "dst"):
            offset = gg.info.offsets[i]
            if i in self.ui_hero:
                del self.ui_hero[i]
            self.ui_hero[i] = InteractiveCanvas.HeroUI(offset, self, i == "src")
        self.ui_hero["src"].brother = self.ui_hero["dst"]
        self.ui_hero["src"].hero_select(hero_id)
        self.ui_hero["dst"].brother = self.ui_hero["src"]

    def calculate(self, buffer_level=0, new_perks=True, new_skills=True):
        self.ui_hero["src"].hero.calculate(self.ui_hero["dst"].hero, buffer_level, new_perks, new_skills)


class LogWnd(Toplevel):
    class _TextHandler(logging.Handler):
        def __init__(self, queue):
            super(LogWnd._TextHandler, self).__init__()
            self.queue = queue

        def emit(self, record):
            self.queue.put(self.format(record))

    def __init__(self, parent):
        super(LogWnd, self).__init__(parent)
        self.queue = Queue()

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.title("日志记录")

        self.log_box = scrolledtext.ScrolledText(self, state='disabled', height=60, width=80)
        self.log_box.configure(font='TkFixedFont')
        self.log_box.grid(column=0, row=0, sticky="NEWS")
        text_handler = LogWnd._TextHandler(self.queue)
        logger = logging.getLogger()
        logger.addHandler(text_handler)
        self.after(0, self.append_msg)

    def append_msg(self):
        msgs = []

        while(True):
            try:
                msg = self.queue.get_nowait()
                msgs.append(msg)
            except Empty:
                break

        if len(msgs) > 0:
            msgs.append("")
            self.log_box.configure(state="normal")
            self.log_box.insert(END, "\n".join(msgs))
            self.log_box.configure(state="disabled")
            self.log_box.yview(END)

        self.after(100, self.append_msg)


class MainWnd(Tk):
    def __init__(self, *args):
        super(MainWnd, self).__init__(*args)
        self.lock = Lock()

        self.log_wnd = LogWnd(self)
        self.log_wnd.update()
        self.log_wnd.geometry("+{}+{}".format(per.log_x, per.log_y))
        self.title(TITLE)
        self.resizable(False, False)

        self.interactive_canvas = InteractiveCanvas(self)
        self.interactive_canvas.grid(column=0, row=0, sticky="w")

        self.status_text = Label(self, text="已启动", border=1, relief="sunken", padding=2, font=("TkFixedFont", 10))
        self.status_text.grid(column=0, row=2, sticky="ew")
        self.status_prog = Progressbar(self)

        self.protocol("WM_DELETE_WINDOW", self.on_close)

        self._build_top_menu()
        self.update()
        self.geometry("+{}+{}".format(per.main_x, per.main_y))
        self._asking_game_data()

    def on_close(self):
        per.main_x = self.winfo_x()
        per.main_y = self.winfo_y()
        per.log_x = self.log_wnd.winfo_x()
        per.log_y = self.log_wnd.winfo_y()
        per.save()
        self.destroy()

    def _build_top_menu(self):
        self.top_menu = Menu(self)
        self.config(menu=self.top_menu)
        self.top_menu.add_command(label="计算概率", command=self._on_menu_calculate)
        self.top_menu.add_command(label="", command=self._on_menu_showlog)
        per.show_log = not per.show_log
        self._on_menu_showlog()

    def _on_menu_calculate(self):
        # TODO
        buffer_level = 0
        new_perks = True
        new_skills = True
        self.interactive_canvas.calculate(buffer_level, new_perks, new_skills)

    def _on_menu_showlog(self, event=None):
        if per.show_log:
            new_text = "显示日志"
            self.log_wnd.withdraw()
        else:
            new_text = "隐藏日志"
            self.log_wnd.deiconify()
            self.focus_set()

        per.show_log = not per.show_log
        self.top_menu.entryconfig(2, label=new_text)

    def _build_skill_gui(self):
        self.status_prog.grid_forget()
        self.status_text.grid(column=0, row=2, sticky="ew", columnspan=2)
        self.status_text.config(text="游戏数据加载完毕")

        self.interactive_canvas.load_bg()
        self.interactive_canvas.load_ui("Mardigo")

    def _asking_game_data(self):
        self.withdraw()
        #h5_path = filedialog.askdirectory(title="请选择英雄无敌5安装文件夹", initialdir=per.last_path)

        h5_path = "F:\\games\\TOE31\\"
        if h5_path == "":
            messagebox.showerror(TITLE, "本程序依赖已安装的英雄无敌5游戏数据！\n无游戏数据，退出。")
            return self.on_close()

        per.last_path = h5_path

        self.deiconify()
        self.status_text.grid(column=0, row=2, sticky="we", columnspan=1)
        self.status_prog.grid(column=1, row=2, sticky="we")
        gg.info = None
        raw_data = RawData(h5_path)
        game_info = GameInfo()
        Thread(target=self._ask_game_data_thread, args=(raw_data, game_info)).start()
        self.after(10, self._ask_game_data_after, raw_data, game_info)

    def _ask_game_data_thread(self, raw_data, game_info):
        try:
            raw_data.run()
        except ValueError as e:
            with self.lock:
                gg.info = e
            return

        try:
            game_info.run(raw_data)
        except ValueError as e:
            with self.lock:
                gg.info = e
            return

        with self.lock:
            gg.info = game_info

    def _ask_game_data_after(self, raw_data, game_info):
        with self.lock:
            if type(gg.info) is ValueError:
                self.withdraw()
                messagebox.showerror(TITLE, str(gg.info) + "，\n请检查是否是正确的英雄无敌5安装文件夹")
                self._asking_game_data()
            elif type(gg.info) is GameInfo:
                self._build_skill_gui()
            else:
                status_text = ""
                prog_value = 0.0
                total_weight = RawData.get_time_weightage() + GameInfo.get_time_weightage()

                if game_info.get_stage() is None:
                    status_text = raw_data.get_stage()
                    prog_value = raw_data.get_progress() * 100 * \
                        RawData.get_time_weightage() / total_weight
                else:
                    status_text = game_info.get_stage()
                    prog_value = game_info.get_progress() * 100 * GameInfo.get_time_weightage() / total_weight \
                        + RawData.get_time_weightage() * 100 / total_weight

                prog_value = 100.00 if prog_value > 100.00 else prog_value
                status_text = f"{status_text}, 总进度{prog_value:.2f}%"
                status_text += (65 - len(status_text)) * " "
                self.status_text.config(text=status_text)
                self.status_prog.config(value=prog_value)
                self.after(10, self._ask_game_data_after, raw_data, game_info)
