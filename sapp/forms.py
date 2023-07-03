# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import npyscreen
from slurm_config import SlurmConfig, SubmitConfig, Database, utils
from gpustat import get_card_list

class MuteRoundCheckBox(npyscreen.RoundCheckBox):
    False_box = ' - '
    True_box  = ' - '

class MuteSelectOne(npyscreen.SelectOne):
    _contained_widgets = MuteRoundCheckBox

class MuteTitleSelectOne(npyscreen.TitleMultiLine):
    _entry_type = MuteSelectOne

class MenuForm(npyscreen.FormBaseNew):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        command = parentApp.command
        self.command = " ".join(command) if isinstance(command, list) else command
        preview = "Run with the same setting as you last time use SAPP without a job name. A quick entry for fast job submission."
        if parentApp.database.recent:
            preview = f"Preview: {' '.join(utils.get_command(parentApp.database.recent, 'srun'))}" 
            if parentApp.database.recent.task in (1, 3):
                preview = 'Preview: sbatch' + preview[13:]
        self.options = [
            ("Execute with the most recent setting", preview),
            ("Select from pre-defined settings...", "Run with an existing setting. You may further specify the job name and more details."),
            ("Create a new setting...", "Create a new setting then run the job. SAPP will remember the setting so that you could quickly start next time."),
            ("Create a new setting for one-time execution...", "Create a new setting then run the job. SAPP will NOT remember this setting."),
            ("Start from an existing setting...", "Edit an existing setting then run the job. Your change will NOT be saved to the database."),
            ("Edit existing settings...", "Edit an existing setting then run the job. Your change will be saved to the database."),
            ("Remove existing settings...", "Remove existing settings in the database."),
            ("Exit", "Leave SAPP without doing anything.")
        ]
        self.preview = preview
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def afterEditing(self):
        if self.field.value[0] == 1:
            self.parentApp.setNextForm('select_config')
        elif self.field.value[0] == 2:
            self.parentApp.getForm('new_config').update(None, "Create a new slurm config.")
            self.parentApp.setNextForm('new_config')
        elif self.field.value[0] == 3:
            self.parentApp.getForm('new_config').update(None, "Create a new slurm config for one-time execution.", True)
            self.parentApp.setNextForm('new_config')
        elif self.field.value[0] == 4:
            self.parentApp.setNextForm('edit_run_config')
        elif self.field.value[0] == 5:
            self.parentApp.setNextForm('edit_run_config')
        elif self.field.value[0] == 6:
            self.parentApp.setNextForm('remove_config')
        else:
            self.parentApp.setNextForm(None)

    def while_editing(self, value):
        if self.field.value:
            self.editing = False
    
    def adjust_widgets(self):
        self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
        self.explanation.update()
        return super().adjust_widgets()

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Welcome to sapp menu page! Command to execute: {self.command}")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value=self.preview)
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(MuteTitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="MENU", values = [v for v, _ in self.options])

        if not self.parentApp.database.recent:
            self.field.value = [3]


class RoundCheckBox(npyscreen.RoundCheckBox):
    True_box  = '(x)'

class SelectOne(npyscreen.SelectOne):
    _contained_widgets = RoundCheckBox

class TitleSelectOne(npyscreen.TitleMultiLine):
    _entry_type = SelectOne

class CheckBox(npyscreen.CheckBox):
    True_box  = '[x]'

class MultiSelect(npyscreen.MultiSelect):
    _contained_widgets = CheckBox

class TitleMultiSelect(npyscreen.TitleMultiLine):
    _entry_type = MultiSelect

class FormMultiPageAction(npyscreen.FormMultiPageAction):
    def switch_page(self, page, display=True):
        self._widgets__ = self._pages__[page]
        self._active_page = page
        self.editw = 0

        # skip the head line
        if hasattr(self, "cycle_widgets"):
            if not self.cycle_widgets:
                r = list(range(self.editw, len(self._widgets__)))
            else:
                r = list(range(self.editw, len(self._widgets__))) + list(range(0, self.editw))
            for n in r:
                if self._widgets__[n].editable and not self._widgets__[n].hidden: 
                    self.editw = n
                    break
        
        if display:
            self.display(clear=True)
    
    def find_previous_editable(self, *args):
        if self.editw == 0:
            if self._active_page > 0:
                self.switch_page(self._active_page-1)
        
        if not self.editw == 0:     
            # remember that xrange does not return the 'last' value,
            # so go to -1, not 0! (fence post error in reverse)
            for n in range(self.editw-1, -1, -1 ):
                if self._widgets__[n].editable and not self._widgets__[n].hidden: 
                    self.editw = n
                    break
            else:
                if self._active_page > 0:
                    self.switch_page(self._active_page-1)

    def add_existing_widget(self, widget, max_height=None, rely=None, relx=None):
        """Add an existing widget. It can be a weak reference proxy. Mainly for paged forms."""

        if rely is None:
            rely = self.nextrely
        if relx is None:
            relx = self.nextrelx

        if max_height is False:
            max_height = self.curses_pad.getmaxyx()[0] - rely - 1

        _w = widget

        self.nextrely = _w.height + _w.rely 
        self._widgets__.append(_w)

        return _w

    def auto_add(self, *args, **kwargs):
        """Auto split page based on the height."""
        height = kwargs.get("height", kwargs.get("max_height", 1 if len(kwargs.get("name", "")) < 12 else 2))
        if self.nextrely + height > self.lines - 4:
            self.add_page()
            self.add_existing_widget(self.text)
            self.add_existing_widget(self.explanation)
            self.add_existing_widget(self.empty_line)

        comments = kwargs.pop("comments", "")
        _w = self.add(*args, **kwargs)
        _w.comments = comments

        return _w
    
    def while_editing(self, value):
        self.explanation.value = getattr(self._widgets__[self.editw], "comments", "")
        self.explanation.update()
    
    def create(self, text):
        self.text = self.add(npyscreen.FixedText, editable=False, value=text)
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.empty_line = self.add(npyscreen.FixedText, editable=False, value="")


class SlurmConfigForm(FormMultiPageAction):
    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        self.slurm_config = SlurmConfig()
        self.freeze_name = False
        self.greetings = "<greetings>"

        self.card_list = get_card_list()
        self.partitions = list(self.card_list.keys())
        self.cards = {p: list(self.card_list[p].keys()) for p in self.partitions}

        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        # write to config
        if not self.freeze_name:
            self.slurm_config.name = self.get_widget("name").value
        self.slurm_config.nodes = int(self.get_widget("nodes").value)
        self.slurm_config.ntasks = int(self.get_widget("ntasks").value)
        self.slurm_config.disable_status = self.get_widget("disable_status").value == [0]
        self.slurm_config.unbuffered = self.get_widget("unbuffered").value == [0]
        p = self.partitions[self.get_widget("partition").value[0]]
        self.slurm_config.partition = p
        self.slurm_config.gpu_type = ["Any Type", *self.cards[p]][self.get_widget("gpu_type").value[0]]
        self.slurm_config.num_gpus = int(self.get_widget("num_gpus").value)
        self.slurm_config.cpus_per_task = int(self.get_widget("cpus_per_task").value)
        self.slurm_config.mem = self.get_widget("mem").value
        self.slurm_config.other = self.get_widget("other").value

        # write to submit config
        self.parentApp.getForm('submit').submit_config.slurm_config = self.slurm_config

        # proceed to submission
        self.parentApp.setNextForm('submit')
    
    def on_cancel(self):
        previous = self.parentApp.getHistory()
        previous = previous[-2] if previous[-1] == 'new_config' else previous[-1]
        if previous == 'MAIN':
            form = self.parentApp.getForm('MAIN')
            form.field.value = None
        self.parentApp.setNextFormPrevious()

    def create(self):
        super().create(self.greetings)
        card_list = self.card_list
        partitions = self.partitions
        cards = self.cards
        
        self.auto_add(npyscreen.TitleText, w_id="name", value=self.slurm_config.name, name = "Name", comments="Config name. The only identifier to store in the database for your own use.", editable=not self.freeze_name)
        self.auto_add(npyscreen.TitleSlider, w_id="nodes", value=self.slurm_config.nodes, lowest=1, out_of=10, name = "Nodes", comments="Request that a minimum of minnodes nodes be allocated to this job.")
        self.auto_add(npyscreen.TitleSlider, w_id="ntasks", value=self.slurm_config.ntasks, lowest=1, out_of=10, name = "# tasks", comments="Specify the number of tasks to run. Unless you know its meaning, do not change.")
        self.auto_add(TitleSelectOne, w_id="disable_status", max_height=2, value=(not self.slurm_config.disable_status), name="Ctrl-C", values = ["Yes", "No"], scroll_exit=True, comments="Disable the display of task status when srun receives a single SIGINT (Ctrl-C). Only useful for srun.")
        self.auto_add(TitleSelectOne, w_id="unbuffered", max_height=2, value=(not self.slurm_config.unbuffered), name="Unbuffered", values = ["Yes", "No"], scroll_exit=True, comments="Always flush the outputs to console. Only useful for srun.")

        height = min(len(partitions), max(5, self.lines-self.text.height-6))
        partition_widget = self.auto_add(TitleSelectOne, w_id="partition", max_height=height, value=([0] if self.slurm_config.partition not in partitions else partitions.index(self.slurm_config.partition)), name="Partition", values = [f"{p} (Available: {sum(card_list[p].values())})" for p in partitions], scroll_exit=True, select_exit=True, comments="Request a specific partition for the resource allocation.")

        height = min(max(len(cards[p]) for p in partitions), max(5, self.lines-self.text.height-6))
        p = partitions[partition_widget.value[0]]
        gpu_type_widget = self.auto_add(TitleSelectOne, w_id="gpu_type", max_height=height, value=([0] if self.slurm_config.gpu_type not in cards[p] else [cards[p].index(self.slurm_config.gpu_type) + 1]), name="GPU Type", values = [f"Any Type (Available: {sum(card_list[p].values())})"] + [f"{c} (Available: {card_list[p][c]})" for c in cards[p]], scroll_exit=True, select_exit=True, comments="GPU type for allocation.")

        def when_value_edited():
            p = partitions[partition_widget.value[0]]
            gpu_type_widget.value = [0]
            gpu_type_widget.values = [f"Any Type (Available: {sum(card_list[p].values())})"] + [f"{c} (Available: {card_list[p][c]})" for c in cards[p]]
            gpu_type_widget.update()
        partition_widget.entry_widget.when_value_edited = when_value_edited

        self.auto_add(npyscreen.TitleSlider, w_id="num_gpus", value=self.slurm_config.num_gpus, lowest=0, out_of=16, name = "# gpus", comments="Number of GPUs to request.")
        self.auto_add(npyscreen.TitleSlider, w_id="cpus_per_task", value=self.slurm_config.cpus_per_task, lowest=1, out_of=32, name = "# cpus per task", comments="Request that ncpus be allocated per process. This may be useful if the job is multithreaded.")
        self.auto_add(npyscreen.TitleText, w_id="mem", value=self.slurm_config.mem, name = "Memory", comments="(Optional) Specify the real memory required per node. E.g. 40G.")
        self.auto_add(npyscreen.TitleText, w_id="other", value=self.slurm_config.other, name = "Other", comments="(Optional) Other command line arguments, such as '--exclude ai_gpu02,ai_gpu04'.")

    def update(self, slurm_config: SlurmConfig = None, greetings: str = "", freeze_name: bool = False):
        self.freeze_name = freeze_name
        self.greetings = greetings

        self.get_widget("name").editable = not freeze_name
        self.get_widget("name").update()
        self.text.value = greetings
        self.text.update()

        if slurm_config is not None:
            self.slurm_config = slurm_config

            if not self.freeze_name:
                self.get_widget("name").value = self.slurm_config.name
            self.get_widget("nodes").value = self.slurm_config.nodes
            self.get_widget("ntasks").value = self.slurm_config.ntasks
            self.get_widget("disable_status").value = [0 if self.slurm_config.disable_status else 1]
            self.get_widget("unbuffered").value = [0 if self.slurm_config.unbuffered else 1]
            self.get_widget("partition").value = [self.partitions.index(self.slurm_config.partition)] if self.slurm_config.partition in self.partitions else [0]
            self.get_widget("gpu_type").value = [0 if self.slurm_config.gpu_type == "Any Type" else (self.cards.get(self.slurm_config.partition, []).index(self.slurm_config.gpu_type) if self.slurm_config.gpu_type in self.cards.get(self.slurm_config.partition, []) else 0)]
            self.get_widget("num_gpus").value = self.slurm_config.num_gpus
            self.get_widget("cpus_per_task").value = self.slurm_config.cpus_per_task
            self.get_widget("mem").value = self.slurm_config.mem
            self.get_widget("other").value = self.slurm_config.other

            for key in ("name", "nodes", "ntasks", "disable_status", "unbuffered", "partition", "gpu_type", "num_gpus", "cpus_per_task", "mem", "other"):
                self.get_widget(key).update()

class SelectConfigForm(npyscreen.ActionFormV2):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        self.options = [
            (s.name, f"Preview: {' '.join(utils.get_command(s, 'srun'))}") for s in parentApp.database.settings
        ]
        if parentApp.database.recent:
            self.options.insert(0, ("RECENT", f"Preview: {' '.join(utils.get_command(parentApp.database.recent.slurm_config, 'srun'))}"))
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        if self.parentApp.database.recent:
            if self.field.value[0] == 0:
                slurm_config = self.parentApp.database.recent.slurm_config
            else:
                slurm_config = self.parentApp.database.settings[self.field.value[0] - 1]
        else:
            slurm_config = self.parentApp.database.settings[self.field.value[0]]
        # write to submit config
        self.parentApp.getForm('submit').submit_config.slurm_config = slurm_config

        # proceed to submission
        self.parentApp.setNextForm('submit')
    
    def on_cancel(self):
        form = self.parentApp.getForm('MAIN')
        form.field.value = None
        self.parentApp.setNextFormPrevious()
    
    def adjust_widgets(self):
        self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
        self.explanation.update()
        return super().adjust_widgets()

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select a setting to execute.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])

class EditRunConfigForm(npyscreen.ActionFormV2):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        self.options = [
            (s.name, f"Preview: {' '.join(utils.get_command(s, 'srun'))}") for s in parentApp.database.settings
        ]
        if parentApp.database.recent:
            self.options.insert(0, ("RECENT", f"Preview: {' '.join(utils.get_command(parentApp.database.recent.slurm_config, 'srun'))}"))
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        if self.parentApp.database.recent:
            if self.field.value[0] == 0:
                slurm_config = self.parentApp.database.recent.slurm_config
            else:
                slurm_config = self.parentApp.database.settings[self.field.value[0] - 1]
        else:
            slurm_config = self.parentApp.database.settings[self.field.value[0]]
        # write to submit config
        self.parentApp.getForm('new_config').update(
            slurm_config,
            greetings = "Edit the slurm config."
        )

        # proceed to submission
        self.parentApp.setNextForm('new_config')
    
    def on_cancel(self):
        form = self.parentApp.getForm('MAIN')
        form.field.value = None
        self.parentApp.setNextFormPrevious()
    
    def adjust_widgets(self):
        self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
        self.explanation.update()
        return super().adjust_widgets()

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select a setting, then proceed to make some changes.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])

class RemoveConfigForm(npyscreen.ActionFormV2):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        self.options = [
            (s.name, f"Preview: {' '.join(utils.get_command(s, 'srun'))}") for s in parentApp.database.settings
        ]
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        self.parentApp.database.remove(self.field.value)
        self.parentApp.setNextForm(None)
    
    def on_cancel(self):
        form = self.parentApp.getForm('MAIN')
        form.field.value = None
        self.parentApp.setNextFormPrevious()
    
    def adjust_widgets(self):
        self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
        self.explanation.update()
        return super().adjust_widgets()

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select the settings to remove from the database.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleMultiSelect, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])

class SubmitForm(FormMultiPageAction):

    def __init__(self, display_pages=True, pages_label_color='NORMAL', *args, **keywords):
        self.submit_config = SubmitConfig()
        super().__init__(display_pages, pages_label_color, *args, **keywords)
    
    def on_ok(self):
        # write to config
        self.submit_config.task = self.get_widget("task").value[0]
        self.submit_config.time = self.get_widget("time").value
        self.submit_config.jobname = self.get_widget("jobname").value
        self.submit_config.output = self.get_widget("output").value
        self.submit_config.error = self.get_widget("error").value

        # proceed to exit
        self.parentApp.setNextForm(None)
    
    def on_cancel(self):
        self.parentApp.setNextFormPrevious()

    def adjust_widgets(self):
        if self._widgets__[self.editw] == self.get_widget("task"):
            self.explanation.value = [
                "Execute with interactive mode (as if you are on the compute node)", 
                "Submit the job and run at background (apply to nohup users)", 
                "Print the command for interactive execution", 
                "Print bash header required to submit an sbatch job"
            ][self.get_widget("task").entry_widget.cursor_line]
            self.explanation.update()
        return super().adjust_widgets()

    def create(self):
        super().create("Submit the job to slurm.")
        
        task = self.auto_add(TitleSelectOne, w_id="task", max_height=4, value=[0], name="Task", values = ["Submit job with srun", "Submit job with sbatch", "Print srun command", "Print sbatch header"], scroll_exit=True, comments="Task to execute. (press arrow keys to show description)", select_exit=True)
        self.auto_add(npyscreen.TitleText, w_id="jobname", name = "Name", comments="(Optional) Job name for slurm. Will appear in squeue.")
        self.auto_add(npyscreen.TitleText, w_id="time", value="0-01:00:00", name = "Time", comments="Limit on the total run time of the job allocation. E.g. 0-01:00:00")
        output = self.auto_add(npyscreen.TitleFilenameCombo, w_id="output", name = "Output", comments="(Optional) The output filename. use %j for job id and %x for job name. You may want to leave it blank for srun.")
        error = self.auto_add(npyscreen.TitleFilenameCombo, w_id="error", name = "Error", comments="(Optional) The stderr filename. use %j for job id and %x for job name. You may want to leave it blank for srun.")

        def when_value_edited():
            database: Database = self.parentApp.database
            if task.value and task.value[0] in (1, 3) and not output.value and not error.value:
                output.value = str(database.base_path / database.identifier / "output.txt")
                error.value = str(database.base_path / database.identifier / "error.txt")
            elif task.value and task.value[0] in (0, 2) \
                and output.value == str(database.base_path / database.identifier / "output.txt") \
                and error.value == str(database.base_path / database.identifier / "error.txt"):
                output.value = None
                error.value = None
        task.when_value_edited = when_value_edited


class SlurmApplication(npyscreen.NPSAppManaged):
    def __init__(self, command):
        self.command = command
        self.database = Database()
        super().__init__()
    
    def onStart(self):
        self.addForm('MAIN', MenuForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('select_config', SelectConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('edit_run_config', EditRunConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('remove_config', RemoveConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('new_config', SlurmConfigForm, name="SAPP", minimum_lines=14, scroll_exit=True)
        self.addForm('submit', SubmitForm, name="SAPP", minimum_lines=14, scroll_exit=True)
    
    def process(self):
        menu = self.getForm('MAIN').field.value[0]
        submit = self.getForm('submit').submit_config
        if menu == 0:
            assert self.database.recent is not None, "If you use SAPP for the first time, please consider creating a new setting first."
            self.database.execute(self.command, self.database.recent)
        elif menu == 1:
            self.database.execute(self.command, submit)
        elif menu == 2:
            self.database.add(submit.slurm_config)
            self.database.execute(self.command, submit)
        elif menu == 3:
            self.database.execute(self.command, submit)
        elif menu == 4:
            self.database.execute(self.command, submit)
        elif menu == 5:
            idx = self.getForm('edit_run_config').field.value[0]
            if self.database.recent and idx != 0:
                self.database.settings[idx-1] = submit.slurm_config
            elif not self.database.recent:
                self.database.settings[idx] = submit.slurm_config
            self.database.execute(self.command, submit)
        elif menu == 6:
            self.database.dump()
            print("Settings successfully removed.")
        elif menu == 7:
            exit(0)
