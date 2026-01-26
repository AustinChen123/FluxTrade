import os
import glob
import importlib.util
import inspect
import logging
import traceback
from typing import Dict, Type, Union
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)

class StrategyLoader:
    @staticmethod
    def scan_directory(path: str) -> Dict[str, Union[Type[BaseStrategy], str]]:
        """
        Scans a directory for Python files and loads subclasses of BaseStrategy.
        Returns a dictionary mapping "FileName::ClassName" to the strategy class.
        In case of loading errors, the value will be the error traceback string.
        """
        strategies = {}
        if not os.path.exists(path):
            logger.warning(f"Directory {path} does not exist.")
            return strategies

        search_path = os.path.join(path, "*.py")
        for file_path in glob.glob(search_path):
            file_name = os.path.basename(file_path)
            if file_name == "__init__.py":
                continue

            module_name = file_name[:-3]
            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    continue
                
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                found_any = False
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, BaseStrategy) and 
                        obj is not BaseStrategy):
                        
                        strategy_id = f"{file_name}::{name}"
                        strategies[strategy_id] = obj
                        logger.info(f"Loaded strategy: {strategy_id}")
                        found_any = True
                
                if not found_any:
                    logger.debug(f"No BaseStrategy subclass found in {file_name}")

            except Exception:
                error_trace = traceback.format_exc()
                logger.error(f"Failed to load module {file_path}:\n{error_trace}")
                # Use a special key pattern for load errors to satisfy the "special status" requirement
                strategies[f"{file_name}::LoadError"] = error_trace
        
        return strategies
