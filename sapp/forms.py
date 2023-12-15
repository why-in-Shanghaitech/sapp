# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import npyscreen
import shlex
from .slurm_config import SlurmConfig, SubmitConfig, Database, utils
from .gpustat import get_card_list

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
        self.command = shlex.join(command) if isinstance(command, list) else command
        preview = "Run with the same setting as you last time use SAPP without a job name. A quick entry for fast job submission."
        if parentApp.database.recent:
            preview = f"Preview: {' '.join(utils.get_command(parentApp.database.recent, 'srun', parentApp.database.identifier, parentApp.database.config))}" 
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
            ("Modify general settings...", "General settings of SAPP. You might want to make some changes so that it fits your environment."),
            ("Exit", "Leave SAPP without doing anything.")
        ]
        self.preview = preview
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def afterEditing(self):
        if self.field.value[0] == 1:
            self.parentApp.setNextForm('select_config')
        elif self.field.value[0] == 2:
            greetings = "Create a new slurm config."
            if not self.parentApp.database.recent:
                greetings = "Welcome to SAPP! To use SAPP for the first time, please create a slurm setting."
            self.parentApp.getForm('new_config').update(None, greetings)
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
        elif self.field.value[0] == 7:
            self.parentApp.setNextForm('general_config')
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
        welcome_message = "Welcome to sapp menu page! Command to execute: "
        text = self.add(npyscreen.FixedText, editable=False, value=welcome_message)
        self.add(npyscreen.FixedText, relx=len(welcome_message)+text.relx, rely=text.rely, color="IMPORTANT", editable=False, value=f"{self.command}")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value=self.preview)
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(MuteTitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="MENU", values = [v for v, _ in self.options])

        if not self.parentApp.database.recent:
            self.field.value = [2]


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

        self.card_list = parentApp.card_list
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

        def avail_of(partition: str, gpu_type: str = None, gpu_count: int = 1) -> int:
            gpu_count = max(1, gpu_count)
            if partition not in card_list:
                return 0
            if gpu_type is None:
                candidates = [value for gpu_type in card_list[partition] for value in card_list[partition][gpu_type]]
            else:
                candidates = card_list[partition].get(gpu_type, [])
            return sum(int(value // gpu_count) for value in candidates)
        
        self.auto_add(npyscreen.TitleText, w_id="name", value=self.slurm_config.name, name = "Name", comments="Config name. Only used by sapp. Later in sapp you may quickly select this config by its name.", editable=not self.freeze_name)
        self.auto_add(npyscreen.TitleSlider, w_id="nodes", value=self.slurm_config.nodes, lowest=1, out_of=10, name = "Nodes", comments="Request that a minimum of minnodes nodes be allocated to this job. Do not change unless you know its meaning.")
        self.auto_add(npyscreen.TitleSlider, w_id="ntasks", value=self.slurm_config.ntasks, lowest=1, out_of=10, name = "# tasks", comments="Specify the number of tasks to run. Do not change unless you know its meaning.")
        self.auto_add(TitleSelectOne, w_id="disable_status", max_height=2, value=(not self.slurm_config.disable_status), name="Ctrl-C", values = ["Yes", "No"], scroll_exit=True, select_exit=True, comments="Disable the display of task status when srun receives a single SIGINT (Ctrl-C). Safe to leave it untouched.")
        self.auto_add(TitleSelectOne, w_id="unbuffered", max_height=2, value=(not self.slurm_config.unbuffered), name="Unbuffered", values = ["Yes", "No"], scroll_exit=True, select_exit=True, comments="Always flush the outputs to console. Only useful for srun. Safe to leave it untouched.")

        height = max(2, min(len(partitions), max(5, self.lines-self.text.height-6)))
        partition_widget = self.auto_add(TitleSelectOne, w_id="partition", max_height=height, value=([0] if self.slurm_config.partition not in partitions else partitions.index(self.slurm_config.partition)), name="Partition", values = [f"{p} (Available: {avail_of(p)})" for p in partitions], scroll_exit=True, select_exit=True, comments="Request a specific partition for the resource allocation.")

        height = max(2, min(max(len(cards[p]) for p in partitions), max(5, self.lines-self.text.height-6)))
        p = partitions[partition_widget.value[0]]
        gpu_type_widget = self.auto_add(TitleSelectOne, w_id="gpu_type", max_height=height, value=([0] if self.slurm_config.gpu_type not in cards[p] else [cards[p].index(self.slurm_config.gpu_type) + 1]), name="GPU Type", values = [f"Any Type (Available: {avail_of(p)})"] + [f"{c} (Available: {avail_of(p, c)})" for c in cards[p]], scroll_exit=True, select_exit=True, comments="GPU type for allocation.")

        def when_value_edited():
            p = partitions[partition_widget.value[0]]
            if p == when_value_edited.old_p:
                return
            when_value_edited.old_p = p
            gpu_type_widget.value = [0]
            gpu_type_widget.values = [f"Any Type (Available: {avail_of(p)})"] + [f"{c} (Available: {avail_of(p, c)})" for c in cards[p]]
            gpu_type_widget.update()
        when_value_edited.old_p = None
        partition_widget.entry_widget.when_value_edited = when_value_edited

        num_gpu_widget = self.auto_add(npyscreen.TitleSlider, w_id="num_gpus", value=self.slurm_config.num_gpus, lowest=0, out_of=16, name = "# gpus", comments="Number of GPUs to request.")

        def when_value_edited_gpu():
            p = partitions[partition_widget.value[0]]
            gpu_count = num_gpu_widget.value
            partition_widget.values = [f"{p} (Available: {avail_of(p, gpu_count=gpu_count)})" for p in partitions]
            gpu_type_widget.values = [f"Any Type (Available: {avail_of(p, gpu_count=gpu_count)})"] + [f"{c} (Available: {avail_of(p, c, gpu_count)})" for c in cards[p]]
            self.display() # redraw the form so that no display problem
        num_gpu_widget.entry_widget.when_value_edited = when_value_edited_gpu

        self.auto_add(npyscreen.TitleSlider, w_id="cpus_per_task", value=self.slurm_config.cpus_per_task, lowest=1, out_of=128, name = "# cpus per task", comments="Request that ncpus be allocated per process. This may be useful if the job is multithreaded.")
        self.auto_add(npyscreen.TitleText, w_id="mem", value=self.slurm_config.mem, name = "Memory", comments="(Optional) Specify the real memory required per node. E.g. 40G.")
        self.auto_add(npyscreen.TitleText, w_id="other", value=self.slurm_config.other, name = "Other", comments="(Optional) Other command line arguments, such as '--exclude ai_gpu02,ai_gpu04'.")
    
    def pre_edit_loop(self):
        super().pre_edit_loop()
        
        self.ok_button.comments = "Proceed to set job-specific details: choose to run with srun or sbatch, set the Internet, etc."
        self.c_button.comments = "Back to the previous menu."

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
            self.get_widget("partition").update() # we must update early to get the correct gpu_type
            self.get_widget("partition").entry_widget.when_value_edited() # update options for gpu_type
            avail_cards = self.cards.get(self.slurm_config.partition, [])
            self.get_widget("gpu_type").value = [0 if self.slurm_config.gpu_type == "Any Type" else ((avail_cards.index(self.slurm_config.gpu_type)+1) if self.slurm_config.gpu_type in avail_cards else 0)]
            self.get_widget("num_gpus").value = self.slurm_config.num_gpus
            self.get_widget("num_gpus").entry_widget.when_value_edited() # update available cards
            self.get_widget("cpus_per_task").value = self.slurm_config.cpus_per_task
            self.get_widget("mem").value = self.slurm_config.mem
            self.get_widget("other").value = self.slurm_config.other

            for key in ("name", "nodes", "ntasks", "disable_status", "unbuffered", "gpu_type", "num_gpus", "cpus_per_task", "mem", "other"):
                self.get_widget(key).update()

class SelectConfigForm(npyscreen.ActionFormV2):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        card_list = parentApp.card_list
        def avail_of(config: SlurmConfig) -> int:
            partition = config.partition
            gpu_type = config.gpu_type
            gpu_count = max(1, config.num_gpus)
            if partition not in card_list:
                return 0
            if gpu_type == 'Any Type':
                candidates = [value for gpu_type in card_list[partition] for value in card_list[partition][gpu_type]]
            else:
                candidates = card_list[partition].get(gpu_type, [])
            return sum(int(value // gpu_count) for value in candidates)

        self.options = [
            (f"{s.name} (Available: {avail_of(s)})", f"Preview: {' '.join(utils.get_command(s, 'srun', general_config=parentApp.database.config))}") for s in parentApp.database.settings
        ]
        if parentApp.database.recent:
            self.options.insert(0, (f"RECENT (Available: {avail_of(parentApp.database.recent.slurm_config)})", f"Preview: {' '.join(utils.get_command(parentApp.database.recent.slurm_config, 'srun', general_config=parentApp.database.config))}"))
        
        self._escape = False # adjust widgets escaper
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        if not self.field.value:
            self.explanation.value = "* Please make a selection to proceed."
            self.explanation.update()
            self._escape = True
            self.parentApp.setNextForm('select_config')
            return

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
        if self._escape == True:
            self._escape = False
        else:
            self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
            self.explanation.update()
        return super().adjust_widgets()
    
    def while_editing(self, *args, **kwargs):
        if hasattr(self._widgets__[self.editw], "comments"):
            self.explanation.value = getattr(self._widgets__[self.editw], "comments", "")
            self.explanation.update()
        return super().while_editing(*args, **kwargs)

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select a setting to execute.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])

    def pre_edit_loop(self):
        super().pre_edit_loop()

        self._added_buttons["ok_button"].comments = "Proceed to set job-specific details: choose to run with srun or sbatch, set the Internet, etc."
        self._added_buttons["cancel_button"].comments = "Back to the main menu."

class EditRunConfigForm(npyscreen.ActionFormV2):

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        card_list = parentApp.card_list
        def avail_of(config: SlurmConfig) -> int:
            partition = config.partition
            gpu_type = config.gpu_type
            gpu_count = max(1, config.num_gpus)
            if partition not in card_list:
                return 0
            if gpu_type == 'Any Type':
                candidates = [value for gpu_type in card_list[partition] for value in card_list[partition][gpu_type]]
            else:
                candidates = card_list[partition].get(gpu_type, [])
            return sum(int(value // gpu_count) for value in candidates)
        
        self.options = [
            (f"{s.name} (Available: {avail_of(s)})", f"Preview: {' '.join(utils.get_command(s, 'srun', general_config=parentApp.database.config))}") for s in parentApp.database.settings
        ]
        if parentApp.database.recent:
            self.options.insert(0, (f"RECENT (Available: {avail_of(parentApp.database.recent.slurm_config)})", f"Preview: {' '.join(utils.get_command(parentApp.database.recent.slurm_config, 'srun', general_config=parentApp.database.config))}"))
        
        self._escape = False # adjust widgets escaper
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        if not self.field.value:
            self.explanation.value = "* Please make a selection to proceed."
            self.explanation.update()
            self._escape = True
            self.parentApp.setNextForm('edit_run_config')
            return

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
        if self._escape == True:
            self._escape = False
        else:
            self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
            self.explanation.update()
        return super().adjust_widgets()
    
    def while_editing(self, *args, **kwargs):
        if hasattr(self._widgets__[self.editw], "comments"):
            self.explanation.value = getattr(self._widgets__[self.editw], "comments", "")
            self.explanation.update()
        return super().while_editing(*args, **kwargs)

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select a setting, then proceed to make some changes.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleSelectOne, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])
    
    def pre_edit_loop(self):
        super().pre_edit_loop()

        self._added_buttons["ok_button"].comments = "Proceed to make some changes to the setting you selected."
        self._added_buttons["cancel_button"].comments = "Back to the main menu."

class RemoveConfigForm(npyscreen.ActionFormV2):
    CANCEL_BUTTON_BR_OFFSET = (2, 16)
    OK_BUTTON_TEXT = 'Delete'

    def __init__(self, name=None, parentApp=None, framed=None, help=None, color='FORMDEFAULT', widget_list=None, cycle_widgets=False, *args, **keywords):
        self.options = [
            (s.name, f"Preview: {' '.join(utils.get_command(s, 'srun', general_config=parentApp.database.config))}") for s in parentApp.database.settings
        ]
        super().__init__(name, parentApp, framed, help, color, widget_list, cycle_widgets, *args, **keywords)

    def on_ok(self):
        self.parentApp.setNextForm(None)
    
    def on_cancel(self):
        form = self.parentApp.getForm('MAIN')
        form.field.value = None
        self.parentApp.setNextFormPrevious()
    
    def adjust_widgets(self):
        self.explanation.value = self.options[self.field.entry_widget.cursor_line][1]
        self.explanation.update()
        return super().adjust_widgets()
    
    def while_editing(self, *args, **kwargs):
        if hasattr(self._widgets__[self.editw], "comments"):
            self.explanation.value = getattr(self._widgets__[self.editw], "comments", "")
            self.explanation.update()
        return super().while_editing(*args, **kwargs)

    def create(self):
        text = self.add(npyscreen.FixedText, editable=False, value=f"Select the settings to remove from the database.")
        self.explanation = self.add(npyscreen.FixedText, editable=False, color="STANDOUT", value="Please select with arrow keys.")
        self.add(npyscreen.FixedText, editable=False, value="")
        height = max(2, self.lines-text.height-6)
        self.field = self.add(TitleMultiSelect, scroll_exit=True, select_exit=True, max_height=height, name="Settings", values = [v for v, _ in self.options])
    
    def pre_edit_loop(self):
        super().pre_edit_loop()

        self._added_buttons["ok_button"].comments = "Confirm and remove the settings you selected."
        self._added_buttons["cancel_button"].comments = "Back to the main menu."

class SubmitForm(FormMultiPageAction):
    CANCEL_BUTTON_BR_OFFSET = (2, 16)
    OK_BUTTON_TEXT = 'Submit'

    def __init__(self, display_pages=True, pages_label_color='NORMAL', *args, **keywords):
        self.submit_config = SubmitConfig()
        super().__init__(display_pages, pages_label_color, *args, **keywords)
    
    def on_ok(self):
        # write to config
        self.submit_config.task = self.get_widget("task").value[0]
        self.submit_config.clash = int(self.get_widget("clash").value) if (self.get_widget("clash").value in (str(i) for i in range(-1, 65536))) else -1
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
        clash = self.auto_add(npyscreen.TitleText, w_id="clash", name = "Internet", value="0", comments="Clash service. -1 for no Internet, 0 for auto port forwarding, otherwise enter a port on the login node.")
        self.auto_add(npyscreen.TitleText, w_id="time", value="0-01:00:00", name = "Time", comments="Limit on the total run time of the job allocation. E.g. 0-01:00:00")
        output = self.auto_add(npyscreen.TitleFilenameCombo, w_id="output", name = "Output", comments="(Optional) The output filename. use %j for job id, %x for job name and %i for timestamp. You may want to leave it blank for srun.")
        error = self.auto_add(npyscreen.TitleFilenameCombo, w_id="error", name = "Error", comments="(Optional) The stderr filename. use %j for job id, %x for job name and %i for timestamp. You may want to leave it blank for srun.")

        def when_value_edited():
            database: Database = self.parentApp.database
            if task.value and task.value[0] in (1, 3) and not output.value and not error.value:
                output.value = str(database.base_path / "%i" / "output.txt")
                error.value = str(database.base_path / "%i" / "error.txt")
                output.update()
                error.update()
            elif task.value and task.value[0] in (0, 2) \
                and output.value == str(database.base_path / "%i" / "output.txt") \
                and error.value == str(database.base_path / "%i" / "error.txt"):
                output.value = None
                error.value = None
                output.update()
                error.update()
            if task.value and task.value[0] in (1, 3) and clash.value == "0":
                clash.value = "-1"
                clash.update()
            elif task.value and task.value[0] in (0, 2) and clash.value == "-1":
                clash.value = "0"
                clash.update()
            
        task.when_value_edited = when_value_edited

    def pre_edit_loop(self):
        super().pre_edit_loop()
        
        self.ok_button.comments = "Submit the job to the slurm system."
        self.c_button.comments = "Back to the previous menu."


class GeneralConfigForm(FormMultiPageAction):
    CANCEL_BUTTON_BR_OFFSET = (2, 16)
    OK_BUTTON_TEXT = 'Apply'

    def __init__(self, display_pages=True, pages_label_color='NORMAL', *args, **keywords):
        self.parentApp = keywords["parentApp"]
        self.general_config: dict = self.parentApp.database.config
        super().__init__(display_pages, pages_label_color, *args, **keywords)
    
    def on_ok(self):
        # write to config
        self.general_config["log_space"] = int(self.get_widget("log_space").value)
        self.general_config["gpu"] = self.get_widget("gpu").value == [1]
        self.general_config["cache"] = self.get_widget("cache").value == [0]

        # proceed to exit
        self.parentApp.setNextForm(None)
    
    def on_cancel(self):
        form = self.parentApp.getForm('MAIN')
        form.field.value = None
        self.parentApp.setNextFormPrevious()

    def create(self):
        super().create("General config for SAPP.")

        self.auto_add(npyscreen.TitleText, w_id="log_space", name = "Log Space", value=str(self.general_config.get("log_space", 200)), comments="Number of logs to keep. By default at ~/.config/sapp. Too small value may lead to task failure. 0 for unlimited.")
        self.auto_add(TitleSelectOne, w_id="gpu", max_height=2, value=[1 if self.general_config.get("gpu", False) else 0], name="GRES", values = ["Submit using --gres=gpu:<type>:<num>", "Submit using --gpus=<type>:<num>"], scroll_exit=True, comments="How to specify the gpu requirement.", select_exit=True)
        self.auto_add(TitleSelectOne, w_id="cache", max_height=2, value=[0 if self.general_config.get("cache", True) else 1], name="Cache", values = ["Cache files in the commands and use independent environment", "Do not cache and submit the file when job is allocated"], scroll_exit=True, comments="Whether to cache the files. This allows you change the files right after submission, no need to wait for allocation.", select_exit=True)

    def pre_edit_loop(self):
        super().pre_edit_loop()
        
        self.ok_button.comments = "Apply the changes to general config."
        self.c_button.comments = "Back to the main menu."


class SlurmApplication(npyscreen.NPSAppManaged):
    def __init__(self, command):
        self.command = command
        self.card_list = get_card_list()
        self.database = Database()
        super().__init__()
    
    def onStart(self):
        self.addForm('MAIN', MenuForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('select_config', SelectConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('edit_run_config', EditRunConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('remove_config', RemoveConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
        self.addForm('new_config', SlurmConfigForm, name="SAPP", minimum_lines=14, scroll_exit=True)
        self.addForm('submit', SubmitForm, name="SAPP", minimum_lines=14, scroll_exit=True)
        self.addForm('general_config', GeneralConfigForm, name="SAPP", minimum_lines=9, scroll_exit=True)
    
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
            deleted = self.database.remove(self.getForm('remove_config').field.value)
            self.database.dump()
            if deleted:
                print(f"Successfully removed {deleted} setting{'s' if deleted > 1 else ''}.")
            else:
                print("No setting is removed.")
        elif menu == 7:
            self.database.config = self.getForm('general_config').general_config
            self.database.dump()
            print(f"Successfully updated SAPP general config.")
        elif menu == 8:
            exit(0)
