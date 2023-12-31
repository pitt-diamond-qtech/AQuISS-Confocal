 # This file is part of pylabcontrol, software for laboratory equipment control for scientific experiments.
    # Copyright (C) <2016>  Arthur Safira, Jan Gieseler, Aaron Kabcenell
    #
    #
    # pylabcontrol is free software: you can redistribute it and/or modify
    # it under the terms of the GNU General Public License as published by
    # the Free Software Foundation, either version 3 of the License, or
    # (at your option) any later version.
    #
    # pylabcontrol is distributed in the hope that it will be useful,
    # but WITHOUT ANY WARRANTY; without even the implied warranty of
    # MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    # GNU General Public License for more details.
    #
    # You should have received a copy of the GNU General Public License
    # along with pylabcontrol.  If not, see <http://www.gnu.org/licenses/>.

import os
import datetime
import pkgutil

def module_name_from_path(folder_name, verbose=False):
    """
    takes in a path to a folder or file and return the module path and the path to the module

    the module is idenitified by
        the path being in os.path, e.g. if /Users/Projects/Python/ is in os.path,
        then folder_name = '/Users/PycharmProjects/pylabcontrol/pylabcontrol/scripts/script_dummy.pyc'
        returns '/Users/PycharmProjects/' as the path and pylabcontrol.scripts.script_dummy as the module

    Args:
        folder_name: path to a file of the form
        '/Users/PycharmProjects/pylabcontrol/pylabcontrol/scripts/script_dummy.pyc'

    Returns:
        module: a string of the form, e.g. pylabcontrol.scripts.script_dummy ...
        path: a string with the path to the module, e.g. /Users/PycharmProjects/

    """
    # strip off endings
    folder_name = folder_name.split('.pyc')[0]
    folder_name = folder_name.split('.py')[0]

    folder_name = os.path.normpath(folder_name)

    path = folder_name + '/'

    package = get_python_package(path)
    # path = folder_name
    module = []

    if verbose:
        print(('folder_name', folder_name))

    # os_sys_path = os.sys.path
    #
    # if os.path.normpath(path) in os_sys_path:
    #     if verbose:
    #         print('warning: path in sys.path!')
    #     os_sys_path.remove(os.path.normpath(path))
    #
    #
    # if verbose:
    #     for elem in os_sys_path:
    #
    #         print('os.sys.path', elem)



    while True:

        path = os.path.dirname(path)

        module.append(os.path.basename(path))
        if os.path.basename(path) == package:
            path = os.path.dirname(path)
            break

        # failed to identify the module
        if os.path.dirname(path) == path:
            path, module = None, None
            break

        if verbose:
            print(('path', path, os.path.dirname(path)))

        # if path == os.path.dirname(path):
        #     if verbose:
        #         print('break --  os.path.dirname(path)', os.path.dirname(path))
        #     # path, module = None, None
        #     break
        #

        if verbose:
            print(('module', module))



    if verbose:
        print(('module', module))

    #occurs if module not found in this path
    if(not module):
        raise ModuleNotFoundError('The path in the .b26 file to this package is not valid')

    # module = module[:-1]
    # print('mod', module)
    # from the list construct the path like b26_toolkit.pylabcontrol.scripts and load it
    module.reverse()
    module = '.'.join(module)

    return module, path

def is_python_package(path):
    """
    checks if folder is a python package or not, i.e. does the folder contain a file __init__.py


    Args:
        path:

    Returns:

        True if path points to a python package
    """

    return os.path.isfile(os.path.join(path, '__init__.py'))

def get_python_package(filename):
    """

    retuns the name of the python package to which the file filename belongs. If file is not in a package returns None

    Note that if the file is in a subpackage, the highest lying package gets returned

    Args:   filename of file for which we would like to find the package
        filename:

    Returns:
        the name of the python package

    """

    package_found = False

    path = os.path.dirname(filename)

    # turn path to file into an array
    path_array = []
    while True:
        path = os.path.dirname(path)
        if path == os.path.dirname(path):
            break
        path_array.append(os.path.basename(path))

    # now successively build up the path and check if its a package
    path = os.path.normpath('/')
    for p in path_array[::-1]:
        path = os.path.join(path, p)

        if is_python_package(path):
            package_found = True
            break

    if package_found:
        return os.path.basename(path)
    else:
        None


def datetime_from_str(string):
    """

    Args:
        string: string of the form YYMMDD-HH_MM_SS, e.g 160930-18_43_01

    Returns: a datetime object

    """


    return datetime.datetime(year=2000+int(string[0:2]), month=int(string[2:4]), day=int(string[4:6]), hour=int(string[7:9]), minute=int(string[10:12]),second=int(string[13:15]))


def explore_package(module_name):
    """
    returns all the packages in the module

    Args:
        module_name: name of module

    Returns:

    """

    packages = []
    loader = pkgutil.get_loader(module_name)
    for sub_module in pkgutil.walk_packages([os.path.dirname(loader.get_filename())],
                                            prefix=module_name + '.'):
        _, sub_module_name, _ = sub_module
        packages.append(sub_module_name)

    return packages



if __name__ == '__main__':

    print(explore_package('b26_toolkit.pylabcontrol.core'))



