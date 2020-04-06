import os
import json
from translate import Translator


DICTIONARY_CACHE_PATH = 'dictionary_cache.json'


class ParserTranslator:

	def _translate_using_cache(self, word, to_lang, from_lang):
		with open(DICTIONARY_CACHE_PATH, "r", encoding='utf8') as f:
			dictionary_cache = json.load(f)

		if from_lang in dictionary_cache and to_lang in dictionary_cache[from_lang]:
			if word in dictionary_cache[from_lang][to_lang]:
				print(f'Translated {word} using the dictionary cache')
				return dictionary_cache[from_lang][to_lang][word]
			else:
				print(f'{word} does not exist in dictionary from {from_lang} to {to_lang}')
		else:
			print(f'Dictionary from {from_lang} to {to_lang} does not exist in cache')

		return None


	def _translate_using_translator(self, word, to_lang, from_lang):
		translator = Translator(to_lang='en', from_lang='he')
		translation = translator.translate(word)
		if translation:
			return translation
		else:
			raise ValueError(f'Error: Could not translate {word} from {from_lang} to {to_lang}.')

	def _write_translation_to_cache(self, word, to_lang, from_lang, translation):
		with open(DICTIONARY_CACHE_PATH, "r", encoding='utf8') as f:
			dictionary_cache = json.load(f)

		dictionary_cache.setdefault(from_lang, dict())
		dictionary_cache[from_lang].setdefault(to_lang, dict())
		dictionary_cache[from_lang][to_lang][word] = translation

		with open(DICTIONARY_CACHE_PATH, "w", encoding='utf8') as f:
			json.dump(dictionary_cache, f)

		print(f'Wrote translation of {word}, which is {translation} to cache')

	def translate_word(self, word, to_lang, from_lang):
		translation = self._translate_using_cache(word, to_lang, from_lang)
		if translation:
			return translation

		translation = self._translate_using_translator(word, to_lang, from_lang)
		self._write_translation_to_cache(word, to_lang, from_lang, translation)

		return translation