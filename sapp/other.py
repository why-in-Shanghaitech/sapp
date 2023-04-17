import curses
from .pick import Picker, pick

__all__ = ['pick', 'opick', 'fill']

KEY_BACKSPACE = (curses.KEY_BACKSPACE, ord('\b'), 127)
KEY_NUMBERS = tuple(range(ord('0'), ord('9')+1))
KEY_ALPHABETS = tuple(range(ord('a'), ord('z')+1)) + (ord('_'), )

def keyFactory(key):
    def func(picker):
        if picker.index == len(picker.options) - 1:
            if key in KEY_BACKSPACE:
                if not picker.options[-1].endswith(': '):
                    picker.options[-1] = picker.options[-1][:-1]
            else:
                picker.options[-1] += chr(key)
        return None
    return func

def opick(options, *args, hint='Others', default='', verify='any', **kwargs):
    """Construct a picker with 'other' option.
    :param verify: Allowed keys. options: 'any', 'number', 'alphabet'
    :param hint: Hint for additional option.
    :param default: Default result for the additional option.
    
    Usage::

      >>> from pick import pick
      >>> title = 'Please choose an option: '
      >>> options = ['option1', 'option2', 'option3']
      >>> option, index = opick(options, title)
    """
    if hint: options.append(hint + ': ' + default)
    picker = Picker(options, *args, **kwargs)

    for key in KEY_BACKSPACE:
        picker.register_custom_handler(key, keyFactory(key))
    
    if verify != 'alphabet':
        for key in KEY_NUMBERS:
            picker.register_custom_handler(key, keyFactory(key))
    
    if verify != 'number':
        for key in KEY_ALPHABETS:
            picker.register_custom_handler(key, keyFactory(key))
    
    option, index = picker.start()

    if isinstance(option, str) and option.startswith(hint + ': '):
        option = option[len(hint + ': '):]

    return option, index

def fill(*args, verify='any', default='', **kwargs):
    """Construct a blank-filling picker.
    :param verify: Allowed keys. options: 'any', 'number', 'alphabet'
    
    Usage::

      >>> from other import fill
      >>> title = 'Please type in the solution: '
      >>> answer = fill(title, verify='number')
    """
    return opick([], *args, hint='Answer', verify=verify, default=default, indicator=' ', **kwargs)[0]
