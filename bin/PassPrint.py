class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class PassPrint:

    def __init__(self):
        self.__indent = 0

    def msg(self, msg, indent=-1, color=None, newline=True):
        if indent < 0:
            indent = self.__indent
        to_print = (
            (color if color else "") +
            (indent * " ") +
            str(msg) +
            (colors.ENDC if color else "")
        )
        if newline:
            print(to_print)
        else:
            print(to_print),

    def warn(self, msg, indent=-1):
        self.msg(msg, indent, colors.WARNING)

    def error(self, msg, indent=-1):
        self.msg(msg, indent, colors.FAIL)

    def indent(self, indent):
        if indent >= 0:
            self.__indent = indent

    def increase_indent(self):
        self.__indent += 2

    def decrease_indent(self):
        self.__indent -= 2
        if self.__indent < 0:
            self.__indent = 0