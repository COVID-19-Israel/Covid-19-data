import os
import json
from translate import Translator
import logging


DICTIONARY_CACHE_PATH = os.path.join(os.path.dirname(__file__),"dictionary_cache.json")
CHARS_IN_NUMBERS = [",", "%", "$", "-", " ", "(", ")"]


class ParserTranslator:
    def __init__(self, to_lang, from_lang):
        self.to_lang = to_lang
        self.from_lang = from_lang

    def _translate_using_cache(self, word):
        with open(DICTIONARY_CACHE_PATH, "r", encoding="utf8") as f:
            dictionary_cache = json.load(f)

        if (
            self.from_lang in dictionary_cache
            and self.to_lang in dictionary_cache[self.from_lang]
        ):
            if word in dictionary_cache[self.from_lang][self.to_lang]:
                return dictionary_cache[self.from_lang][self.to_lang][word]
        else:
            logging.error(
                f"Dictionary from {self.from_lang} to {self.to_lang} does not exist in cache"
            )

        return None

    def _translate_using_translator(self, word):
        translator = Translator(to_lang=self.to_lang, from_lang=self.from_lang)
        translation = translator.translate(word)
        if translation:
            return translation
        else:
            return ""

    def _write_translation_to_cache(self, word, translation):
        with open(DICTIONARY_CACHE_PATH, "r", encoding="utf8") as f:
            dictionary_cache = json.load(f)

        dictionary_cache.setdefault(self.from_lang, dict())
        dictionary_cache[self.from_lang].setdefault(self.to_lang, dict())
        dictionary_cache[self.from_lang][self.to_lang][word] = translation

        with open(DICTIONARY_CACHE_PATH, "w", encoding="utf8") as f:
            json.dump(dictionary_cache, f)
        if translation:
            logging.debug(
                f"Wrote translation of {word}, which is {translation} to cache"
            )

    @staticmethod
    def _is_number(word):
        temp_word = ParserTranslator._clean_numbers(word)
        try:
            float_word = float(temp_word)
        except ValueError:
            return False

        return True

    @staticmethod
    def _clean_numbers(number):
        cleaned_number = number
        for char in CHARS_IN_NUMBERS:
            cleaned_number = cleaned_number.replace(char, "")
        return cleaned_number

    def translate_word(self, word):
        if not ParserTranslator._is_number(word):
            translation = self._translate_using_cache(word)
            if translation:
                return translation
            translation = self._translate_using_translator(word)
            self._write_translation_to_cache(word, translation)

            return translation
        else:
            return word
