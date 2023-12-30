import sys
import shutil
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from threading import RLock

lock = RLock()

path_routes = {}

transliteration_map = {
    ord('А'): 'A', ord('Б'): 'B', ord('В'): 'V', ord('Г'): 'G', ord('Д'): 'D',
    ord('Е'): 'E', ord('Ё'): 'Yo', ord('Ж'): 'Zh', ord('З'): 'Z',
    ord('И'): 'I',
    ord('Й'): 'Y', ord('К'): 'K', ord('Л'): 'L', ord('М'): 'M', ord('Н'): 'N',
    ord('О'): 'O', ord('П'): 'P', ord('Р'): 'R', ord('С'): 'S', ord('Т'): 'T',
    ord('У'): 'U', ord('Ф'): 'F', ord('Х'): 'Kh', ord('Ц'): 'Ts',
    ord('Ч'): 'Ch',
    ord('Ш'): 'Sh', ord('Щ'): 'Shch', ord('Ъ'): "'", ord('Ы'): 'Y',
    ord('Ь'): "'",
    ord('Э'): 'E', ord('Ю'): 'Yu', ord('Я'): 'Ya',
    ord('а'): 'a', ord('б'): 'b', ord('в'): 'v', ord('г'): 'g', ord('д'): 'd',
    ord('е'): 'e', ord('ё'): 'yo', ord('ж'): 'zh', ord('з'): 'z',
    ord('и'): 'i',
    ord('й'): 'y', ord('к'): 'k', ord('л'): 'l', ord('м'): 'm', ord('н'): 'n',
    ord('о'): 'o', ord('п'): 'p', ord('р'): 'r', ord('с'): 's', ord('т'): 't',
    ord('у'): 'u', ord('ф'): 'f', ord('х'): 'kh', ord('ц'): 'ts',
    ord('ч'): 'ch',
    ord('ш'): 'sh', ord('щ'): 'shch', ord('ъ'): "'", ord('ы'): 'y',
    ord('ь'): "'",
    ord('э'): 'e', ord('ю'): 'yu', ord('я'): 'ya',
    ord('Ґ'): 'G', ord('Є'): 'Ye', ord('І'): 'I', ord('Ї'): 'Yi',
    ord('ґ'): 'g',
    ord('є'): 'ie', ord('і'): 'i', ord('ї'): 'i',
    ord('А'.lower()): 'a', ord('Б'.lower()): 'b', ord('В'.lower()): 'v',
    ord('Г'.lower()): 'g',
    ord('Д'.lower()): 'd', ord('Е'.lower()): 'e', ord('Ё'.lower()): 'yo',
    ord('Ж'.lower()): 'zh',
    ord('З'.lower()): 'z', ord('И'.lower()): 'i', ord('Й'.lower()): 'y',
    ord('К'.lower()): 'k',
    ord('Л'.lower()): 'l', ord('М'.lower()): 'm', ord('Н'.lower()): 'n',
    ord('О'.lower()): 'o',
    ord('П'.lower()): 'p', ord('Р'.lower()): 'r', ord('С'.lower()): 's',
    ord('Т'.lower()): 't',
    ord('У'.lower()): 'u', ord('Ф'.lower()): 'f', ord('Х'.lower()): 'kh',
    ord('Ц'.lower()): 'ts',
    ord('Ч'.lower()): 'ch', ord('Ш'.lower()): 'sh', ord('Щ'.lower()): 'shch',
    ord('Ъ'.lower()): "'",
    ord('Ы'.lower()): 'y', ord('Ь'.lower()): "'", ord('Э'.lower()): 'e',
    ord('Ю'.lower()): 'yu',
    ord('Я'.lower()): 'ya', ord('Ґ'.lower()): 'g', ord('Є'.lower()): 'ie',
    ord('І'.lower()): 'i',
    ord('Ї'.lower()): 'i'
}

file_paths = []
expected_extensions = {
    "images": {"jpeg", "png", "jpg", "svg"},
    "videos": {"avi", "mp4", "mov", "mkv"},
    "documents": {"doc", "docx", "txt", "pdf", "xlsx", "pptx"},
    "music": {"mp3", "ogg", "wav", "amr"},
    "archives": {"zip", "rar", "tar"},
    "undefined": {},
}
actual_file_paths = {
    "images": [],
    "videos": [],
    "documents": [],
    "music": [],
    "archives": [],
    "undefined": [],
}

sorted_files = {
    "images": [],
    "videos": [],
    "documents": [],
    "music": [],
    "archives": [],
    "undefined": [],
}

sorted_extensions = {
    "known_extensions_found": set(),
    "unknown_extensions_found": set(),
}


def get_path_from_cli() -> pathlib.Path:
    """
    Get Path argument from the command line, during run of the script.
    :return: Path object which leads to the folder which has to be sorted.
    """
    if len(sys.argv) < 2:
        raise NotImplementedError(
            "Absence of mandatory Path parameter in CLI. Example of "
            "command to successfully run the script: python sort.py "
            "/user/Desktop/Мотлох"
        )
    return pathlib.Path(sys.argv[1])


def normalize(name: str, is_file: bool = True) -> str:
    """
    Normalizing file or folder in the way of change of its name by
    transliteration approach.
    :param name: Name of the file or folder which has to be normalized.
    :param is_file: Define is it file, otherwise - considered as a folder.
    :return: Normalized name of the file or folder.
    """
    if "." in name and is_file:
        name = name.rsplit(".", maxsplit=1)
    else:
        name = [name, ""]
    name[0] = name[0].translate(transliteration_map)
    normalized_part = "".join(
        ["_" if not char.isalnum() else char for char in name[0]])
    if name[1]:
        return ".".join([normalized_part, name[1]])
    return normalized_part


def sort_paths_by_filetype(paths: list[pathlib.Path]) -> None:
    """
    Sort paths of files by their extensions in an existing dict
    'actual_file_paths'.
    :param paths: Paths which have to be sorted in the 'actual_file_paths'.
    :return: None.
    """
    for path in paths:
        if set(path.parts).intersection(
                set(actual_file_paths.keys())
        ):
            continue
        current_name = pathlib.Path(path).name
        if "." not in current_name:
            actual_file_paths["undefined"].append(path)
            continue
        for k, v in expected_extensions.items():
            if current_name.rsplit(".", maxsplit=1)[1].lower() in v:
                actual_file_paths[k].append(path)
                break
        else:
            actual_file_paths["undefined"].append(path)


def make_dirs_for_sorted_file_paths(root_path: pathlib.Path) -> None:
    """
    Create directories for sorted files.
    :param root_path: Path which used for creation of directories for sorted
    files.
    :return: None.
    """
    for key in actual_file_paths.keys():
        if actual_file_paths.get(key, None):
            if not pathlib.Path(root_path / pathlib.Path(key)).exists():
                pathlib.Path(root_path / pathlib.Path(key)).mkdir(
                    parents=True, exist_ok=True)


def move_files_to_its_category_dir() -> None:
    """
    Move files to related folder by type.
    :return: None.
    """
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for old, new in path_routes.items():
            future = executor.submit(move_file, old, new)
            futures.append(future)

    for future in as_completed(futures):
        future.result()


def store_new_paths(root_path: pathlib.Path):
    # with lock:
    global path_routes
    for category, f_paths in actual_file_paths.items():
        for file_path in f_paths:
            if set(file_path.parts).intersection(
                    set(actual_file_paths.keys())
            ):
                continue
            new_file_path = root_path / pathlib.Path(category) / \
                            f"{file_path.stem}{file_path.suffix}"
            # handle copies
            if new_file_path.exists() or new_file_path in path_routes.values():
                path_routes |= {file_path: None}
                continue
                # index = 1
                # while True:
                #     new_file_path = \
                #         root_path / pathlib.Path(category) / \
                #         f"{file_path.stem}({index}){file_path.suffix}"
                #     if new_file_path in path_routes.values():
                #         index += 1
                #         print(new_file_path)
                #         continue
                #     break
            path_routes |= {file_path: new_file_path}


def move_file(current_path, new_path):
    # move file to category dir
    try:
        if new_path:
            current_path.replace(new_path)
        else:
            current_path.unlink()
    except FileExistsError as e:
        print(path_routes)
        print(e)


def delete_empty_folders_recursive(root_path: pathlib.Path) -> None:
    """
    Deletion of empty folders recursively by the root path which was passed as
    argument in the CLI.
    :param root_path: Path which was passed in the CLI as an argument and used
    as the root path for deletion recursively.
    :return: None.
    """
    for folder in sorted(pathlib.Path(root_path).glob('**'), reverse=True):
        if folder in actual_file_paths.keys():
            continue
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()


def extract_and_remove_archives(root_path: pathlib.Path) -> None:
    """
    Extract archives in a folder with similar name by similar path with
    deletion of archive file.
    :param root_path: Path which was passed in the CLI as an argument and used
    as the root path for deletion recursively.
    :return: None.
    """
    path = root_path / pathlib.Path("archives")
    for archive_path in path.glob('*'):
        if archive_path.is_file():
            try:
                shutil.unpack_archive(
                    filename=str(archive_path),
                    extract_dir=str(archive_path.with_suffix(''))
                )
                archive_path.unlink()
            except shutil.ReadError:
                pass


def get_sorted_extensions(root_path: pathlib.Path) -> None:
    """
    Get sorted extensions in a global dict.
    :param root_path: Path which was passed in the CLI as an argument and used
    as the root path for deletion recursively.
    :return: None.
    """
    for path in list(pathlib.Path(root_path).rglob("*")):
        if path.is_dir():
            continue
        if "undefined" not in path.parts:
            sorted_extensions["known_extensions_found"].add(path.suffix)
        else:
            sorted_extensions["unknown_extensions_found"].add(path.suffix)


def get_sorted_files(root_path: pathlib.Path) -> None:
    """
    Get sorted file names in a global dict.
    :param root_path: Path which was passed in the CLI as an argument and used
    as the root path for deletion recursively.
    :return: None.
    """
    for path in list(pathlib.Path(root_path).rglob('*')):
        if path.is_file():
            key = list(set(path.parts).intersection(set(sorted_files.keys())))
            if not key:
                print(path.parts)
            sorted_files[key[0]].append(path.name)


def sort_files_by_path() -> dict:
    """
    Purposes:
    - Normalize folders and files (rename with transliteration name).
    - Creates folders for different file types.
    - Moves files to appropriate folders based on file type.
    - Deletion of empty folders.
    - Unpacking archives by their paths to folders with similar names with
      further deletion of archives.
    :return: list of files by their category type, list of known extensions in
    each category type, list of unknown extensions which were found.
    """
    # get path from arg
    input_path = get_path_from_cli()
    # normalize names
    for path in pathlib.Path(input_path).rglob('*'):
        # Generate the new path with the desired name
        if path.parent.name not in set(actual_file_paths.keys()):
            new_path = path.parent / normalize(
                name=path.name, is_file=pathlib.Path(path).is_file()
            )
            # Rename the folder\file
            renamed_path = path.rename(new_path)
            # Get paths of files
            if pathlib.Path.is_file(renamed_path):
                file_paths.append(renamed_path)

    sort_paths_by_filetype(paths=file_paths)
    make_dirs_for_sorted_file_paths(root_path=input_path)
    store_new_paths(root_path=input_path)
    move_files_to_its_category_dir()
    delete_empty_folders_recursive(root_path=input_path)
    extract_and_remove_archives(root_path=input_path)
    get_sorted_files(root_path=input_path)
    get_sorted_extensions(root_path=input_path)
    return sorted_files | sorted_extensions


if __name__ == "__main__":
    start_time = time.time()
    print(sort_files_by_path())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time} seconds")
