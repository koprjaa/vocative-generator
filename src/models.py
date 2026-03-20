# Project: Vocative Generator
# File:    src/models.py
#
# Description:
# Defines NameResult, the typed record of a single name's vocative form, optional split name parts, success flag, and errors.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# Created: 2025-12-14
#
# License: MIT

from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class NameResult:
    original_name: str
    vocative: str
    first_name: str
    surname: str
    success: bool
    error_message: Optional[str] = None

    @classmethod
    def from_vocative(cls, original_name: str, vocative: str) -> 'NameResult':
        first_name, surname = cls.split_vocative(vocative)
        # Empty or unchanged surface form counts as failure for downstream metrics.
        success = bool(vocative and vocative.strip().lower() != original_name.strip().lower())
        return cls(
            original_name=original_name,
            vocative=vocative.strip() if vocative else original_name,
            first_name=first_name,
            surname=surname,
            success=success
        )

    @classmethod
    def error(cls, original_name: str, error_message: str) -> 'NameResult':
        return cls(
            original_name=original_name,
            vocative=original_name,
            first_name='',
            surname='',
            success=False,
            error_message=error_message
        )

    @staticmethod
    def split_vocative(vocative: str) -> Tuple[str, str]:
        if not vocative:
            return "", ""
        parts = vocative.strip().split()
        if not parts:
            return "", ""
        if len(parts) <= 1:
            return parts[0], ""
        surname = parts[-1]
        first_names = " ".join(parts[:-1])
        return first_names, surname
