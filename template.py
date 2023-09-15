"""
This template script is an example script that is used to show how to structure the code.

This script can be copied and pasted to create scripts. If done this way, please remove
lines starting with #####, and replace this docstring with the appropriate content.

This docstring is used to describe what the module is about, and should be a concise
summary as opposed to a long essay. For the purpose of Sphinx docs, please make sure
to add "Args:", "Returns:", and "Examples:" sections where appropriate.
Sphinx will automatically pick these up and generate documentation for you.

To auto-generate docstrings for functions and classes, any auto-docstring generating tool
can be used, as long as it generates it in google format. The google format can also be
picked up by Sphinx. The autogenerator will not generate the Examples section, and this
section is optional, but recommended for complex functions or basic use-cases for
entry-point functions.

For some general rules, please use two spaces between major sections of code such as
between the "Imports and Constants" and "Code" section, or between two classes.

For TODO statements, place them where appropriate, but have the format of the statement
as the following:
#TODO - <dev_name_in_lower_case> - <task_description>
The reason for this format is that it allows the devs to easily track who the TODO task
belongs to.

As a general rule, use logging over printing for displaying of values.

You may add comments in code, but keep commenting to a minimum.
"""


#####Place all the imports under the "Imports" section, then use isort to sort
#####imports automatically

# ---Imports
import logging

# ---Constants
#####These are globals or constants that are used throughout the module
#####and is specific to the module. If there are multiple modules using
#####the same constants, please create a separate module for them.
logger = logging.getLogger("simple_example")
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ExampleClass:
    """The ExampleClass that shows how the code should be layed out.

    Every class must have a docstring describing the class and the attributes that it has.
    After the class docstring, the __init__() method must come after with the signature
    layed out as defined in this class.

    After the init class, place the public methods afterwards, followed by private methods.

    Unless written by an external library, avoid implementations that can cause method cascading:
    For example avoid implementing for these situtations: obj.method1().method2().method3()
    Rather, try implementing class code to encourage individual method calls:
    obj.method1()
    obj.method2()
    obj.method3()

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
        opt_list_var: list = None,
        opt_dict_var: dict = None,
    ) -> None:
        """Briefly describe what the init function does

        Args:
            var1 (int): [write description].
            var2 (str): [write description].
            var3 (int|float): [write description].
            var4 (str): [write description]. Defaults to "".
            opt_list_var (list): a list that [write description]. Defaults to None.
            opt_dict_var (dict): a dict that [write description]. Defaults to None.

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
        """[Write method description].

        Args:
            some_var (int): [write description].

        Returns:
            None
        """
        logger.debug("public_method1")  #####this is just to fill a statement

    def public_method2(
        self,
        another_var: list,
    ) -> int:
        """[Write method description]. [Briefly describe what it is returning other than its dtype].

        Args:
            another_var (list): [write description].

        Returns:
            [describe what the return value is]
        """
        logger.debug("public_method2")  #####this is just to fill a statement

    def _private_method1(
        self,
        input_dict: dict,
        my_bool: bool,
    ) -> None:
        """[Write method description].

        Args:
            input_dict (dict): [write description].
            my_bool (bool): [write description].

        Returns:
            None
        """
        logger.debug("_private_method1")  #####this is just to fill a statement


def example_function(
    foo: int,
    opt_list_var: list = None,
    opt_dict_var: dict = None,
) -> None:
    """[Write method description].

    Please note that for default values of list and dict as a function parameter, they must be None.
    In other words, it cannot be opt_list_var: list = [] or opt_dict_var: dict = {}

    Args:
        foo (int): [write description].
        opt_list_var (list): [write description]. Defaults to None.
        opt_dict_var (dict): [write description]. Defaults to None.

    Returns:
        None
    """
    logger.debug("example_function")  #####this is just to fill a statement


##### The following two functions show how to and how not to return variables that mutated from a parameter
def example_of_how_to_return_immutable_copies_of_mutable_types(
    input_list: list,
) -> list:
    """[Write method description]. Please note this function is self-evident on what it does,
    therefore does not actually need a docstring.

    Args:
        input_list (list): [write description].

    Returns:
        an immutable list with elements modified
    """
    new_list = input_list.copy()
    new_list[0] += 1
    return new_list


def example_of_how_not_to_return_immutable_copies_of_mutable_types(
    input_list,
) -> list:
    """[Write method description]. Please note this function is self-evident on what it does,
    therefore does not actually need a docstring.

    Args:
        input_list (list): [write description].

    Returns:
        a mutable list with elements modified - NOT RECOMMENDED
    """
    input_list[0] += 1
    return input_list


#####The following function is an example of when the "Examples:" section should
#####be used in the docstring
def function_with_many_parameters(
    p1: int,
    p2: int,
    p3: float,
    p4: float,
    p5: list,
    p6: str,
    p7: str,
    p8: dict = None,
) -> int:
    """[Write method description]

    Args:
            p1 (int): [write description].
            p2 (int): [write description].
            p3 (float): [write description].
            p4 (float): [write description].
            p5 (list): [write description].
            p6 (str): [write description].
            p7 (str): [write description]. Defaults to "".
            p8 (dict): a dict that [write description]. Defaults to None.

    Returns:
        None

    Examples:
        function_with_many_parameters(1, 2, 3.0, 2.0, [0], "foo", "bar"): [describe what this call will do]
    """
    logger.debug("function_with_many_parameters")
