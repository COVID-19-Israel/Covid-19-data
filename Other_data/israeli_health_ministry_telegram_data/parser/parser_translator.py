import json
from translate import Translator


DICTIONARY_CACHE_PATH = 'dictionary_cache.json'


class ParserTranslator:

	def __init__(self, to_lang, from_lang):
		self.to_lang = to_lang
		self.from_lang = from_lang

	def _translate_using_cache(self, word):
		with open(DICTIONARY_CACHE_PATH, "r", encoding='utf8') as f:
			dictionary_cache = json.load(f)

		if self.from_lang in dictionary_cache and self.to_lang in dictionary_cache[self.from_lang]:
			if word in dictionary_cache[self.from_lang][self.to_lang]:
				print(f'Translated {word} using the dictionary cache')
				return dictionary_cache[self.from_lang][self.to_lang][word]
			else:
				print(f'{word} does not exist in dictionary from {self.from_lang} to {self.to_lang}')
		else:
			print(f'Dictionary from {self.from_lang} to {self.to_lang} does not exist in cache')

		return None


	def _translate_using_translator(self, word):
		translator = Translator(to_lang=self.to_lang, from_lang=self.from_lang)
		translation = translator.translate(word)
		if translation:
			return translation
		else:
			print(f'Error: Could not translate word <{word}> from {self.from_lang} to {self.to_lang}.')
			return ''

	def _write_translation_to_cache(self, word, translation):
		with open(DICTIONARY_CACHE_PATH, "r", encoding='utf8') as f:
			dictionary_cache = json.load(f)

		dictionary_cache.setdefault(self.from_lang, dict())
		dictionary_cache[self.from_lang].setdefault(self.to_lang, dict())
		dictionary_cache[self.from_lang][self.to_lang][word] = translation

		with open(DICTIONARY_CACHE_PATH, "w", encoding='utf8') as f:
			json.dump(dictionary_cache, f)

		print(f'Wrote translation of {word}, which is {translation} to cache')

	@staticmethod
	def _is_int(word):
		temp_word = word.replace(',', '')
		try:
			int_word = int(temp_word)
		except ValueError:
			return False

		return True

	@staticmethod
	def _is_float(word):
		temp_word = word.replace(',', '')
		try:
			float_word = float(temp_word)
		except ValueError:
			return False

		return True

	def translate_word(self, word):
		if not ParserTranslator._is_float(word) and not ParserTranslator._is_int(word):
			translation = self._translate_using_cache(word)
			if translation:
				return translation

			print(f'word to translate: <{word}>')
			translation = self._translate_using_translator(word)
			self._write_translation_to_cache(word, translation)

			return translation
		else:
			return word

		