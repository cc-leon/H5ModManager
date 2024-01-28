import logging
from threading import Thread, Lock
from queue import Queue, Empty
from tkinter import END
from tkinter import messagebox, Tk, Menu, scrolledtext, Toplevel, filedialog, LabelFrame, simpledialog
from tkinter.ttk import Label, Progressbar, Style, Checkbutton

from data_parser import RawData, GameInfo
from persistence import per
import data_parser as gg


TITLE = "英雄无敌5MOD兼容工具 by 天天英吧"


class AboutWnd(simpledialog.Dialog):
    def __init__(self, parent, title="关于“英雄无敌5MOD兼容工具”"):
        super(AboutWnd, self).__init__(parent=parent, title=title)

    def body(self, master):
        font = ("TkFixedFont", 11)


    def buttonbox(self):
        pass


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

        font=("TkFixedFont", 11)
        sty = Style(self)
        sty.configure(".", font=font)
        self.num_rows = 0

        self.log_wnd = LogWnd(self)
        self.log_wnd.update()
        self.log_wnd.geometry("+{}+{}".format(per.log_x, per.log_y))

        self.title(TITLE)
        self.resizable(False, False)

        self.status_text = Label(self, text="已启动", border=1, relief="sunken", padding=2, font=("TkFixedFont", 10))
        self.status_text.grid(column=0, row=4, sticky="ew")
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

        self.top_menu.add_command(label="生成兼容文件", command=self._on_menu_createmod)
        self.top_menu.add_command(label="", command=self._on_menu_showlog)
        per.show_log = not per.show_log
        self._on_menu_showlog()
        self.top_menu.add_command(label="帮助与关于",command=self._on_menu_about)

    def _on_menu_createmod(self):
        # TODO
        messagebox.showinfo("aaa", per.get_about_txt())

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

    def _on_menu_about(self):
        self.about_wnd = AboutWnd(self)

    def _build_main_frame(self):
        self.checkboxes = {}

        def _add_labelframe(title: str, row: int, options: list):
            lb = LabelFrame(self, text=title, font=("TkFixedFont", 11))
            lb.grid(column=0, row=row, sticky="ew", padx=10, pady=10)
            cb1 = Checkbutton(lb, text=options[0])
            cb1.grid(column=0, row=0, sticky="w", padx=10, pady=10)
            cb1.state(['!alternate'])
            cb2 = Checkbutton(lb, text=options[1])
            cb2.grid(column=1, row=0, sticky="w", padx=10, pady=0)
            cb2.state(['!alternate'])
            cb3 = Checkbutton(lb, text=options[2])
            cb3.grid(column=2, row=0, sticky="w", padx=10, pady=10)
            cb3.state(['!alternate'])
            return (cb1, cb2, cb3)

        options = ["兼容全英雄MOD", "兼容全魔法MOD（除了探险魔法）", "兼容种族增强MOD"]
        self.checkboxes["scenario"] = _add_labelframe(title="官方战役图兼容选项", row = self.num_rows, options=options)
        self.num_rows += 1
        options[1] = "兼容全魔法全宝物MOD（除了探险魔法和宝物）"
        self.checkboxes["singlemissions"] = _add_labelframe(title="官方单人剧情图兼容选项",
                                                            row = self.num_rows, options=options)
        self.num_rows += 1
        options[1] = "兼容全魔法全宝物MOD（包括探险魔法和宝物）"
        self.checkboxes["multiplayer"] = _add_labelframe(title="官方多人图兼容选项", row = self.num_rows, options=options)
        self.num_rows += 1
        self.checkboxes["customized"] = _add_labelframe(title="玩家自制多人图和随机图兼容选项",
                                                        row = self.num_rows, options=options)
        self.num_rows += 1

        self.status_prog.grid_forget()
        self.status_text.grid(column=0, row=self.num_rows, sticky="ew", columnspan=2)
        self.status_text.config(text="游戏数据加载完毕")

    def _asking_game_data(self):
        self.withdraw()

        h5_path = "F:\\games\\TOE31\\"
        #h5_path = filedialog.askdirectory(title="请选择英雄无敌5安装文件夹", initialdir=per.last_path)
        if h5_path == "":
            messagebox.showerror(TITLE, "本程序依赖已安装的英雄无敌5游戏数据！\n无游戏数据，退出。")
            return self.on_close()

        per.last_path = h5_path

        self.deiconify()
        self.status_text.grid(column=0, row=self.num_rows, sticky="we", columnspan=1)
        self.status_prog.grid(column=1, row=self.num_rows, sticky="we")
        gg.info = None
        raw_data = RawData(h5_path)
        game_info = GameInfo()
        Thread(target=self._ask_game_data_thread, args=(raw_data, game_info)).start()
        self.after(10, self._ask_game_data_after, raw_data, game_info)

    def _ask_game_data_thread(self, raw_data: gg.RawData, game_info: gg.GameInfo):
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

    def _ask_game_data_after(self, raw_data: gg.RawData, game_info: gg.GameInfo):
        with self.lock:
            if type(gg.info) is ValueError:
                self.withdraw()
                messagebox.showerror(TITLE, str(gg.info) + "，\n请检查是否是正确的英雄无敌5安装文件夹")
                self._asking_game_data()
            elif type(gg.info) is GameInfo:
                self._build_main_frame()
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
