# SAPP

`sapp` is a command helper for slurm system. It allows you to act as if you are on compute node. No need to learn how to use slurm.

## How to use

Type `spython` to replace `python` and run your applications.

Use arrow keys to select, type to input.

<div align=center>
    <img src="imgs/demoPlay.gif">
</div>

If you want to execute commands other that `python`, just add `sapp` in front of your command. Actually, `spython` is just a short cut for `sapp python`.

## Install

```sh
pip install sapp-0.3.1-py3-none-any.whl
```

## Uninstall

```sh
pip uninstall sapp
```

## How it works

It just adds the command line arguments for `srun`. Personally I do not like typing a long command, or execute a shell script.

The GPU status query is based on command `sinfo`.

> **Warning**  
> This tool is only tested in Shanghaitech SIST Cluster. It may not suit for all slurm systems.

