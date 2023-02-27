"""
Write module's purpose here
"""


# ---Imports
import logging

# ---Constants
logger = logging.getLogger("simple_example")
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ExampleClass:
    """Write class description

    Attributes:
        var1: [write description].
        var2: [write description].
        var3: [write description].
        var4: [write description].
    """

    def __init__(
        self,
        var1: int,
        var2: str,
        var3: int | float,
        var4: str = "",
    ) -> None:
        """Briefly describe what the init function does

        Args:
            var1 (int): [write description].
            var2 (str): [write description].
            var3 (int|float): [write description].
            var4 (str): [write description]. Defaults to "".
        
        Returns:
            None
        """
        self.var1 = var1
        self.var2 = var2
        self.var3 = var3
        self.var4 = var4

    def public_method1(
        self,
        some_var: int,
    ) -> None:
        #####Remove these comment lines and replace with """ to start the auto-gen of docstring
        #####Remember to do this after implementing the function
        logger.debug("public_method1") #####this is just to fill a statement

    def _private_method1(
        self,
        input_dict: dict,
        my_bool: bool,
    ) -> None:
        #####Remove these comment lines and replace with """ to start the auto-gen of docstring
        #####Remember to do this after implementing the function
        logger.debug("_private_method1") #####this is just to fill a statement


def example_function(
    foo: int,
    opt_list_var: list = None,
    opt_dict_var: dict = None,
) -> None:
    #####Remove these comment lines and replace with """ to start the auto-gen of docstring
    #####Remember to do this after implementing the function
    logger.debug("example_function") #####this is just to fill a statement