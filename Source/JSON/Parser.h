/*
	Copyright(c) 2021-2026 jvde.github@gmail.com

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

#pragma once

#include <vector>
#include <iostream>
#include <memory>
#include <cstring>
#include <cstdint>

#include "JSON.h"
#include "Keys.h"

namespace JSON
{
	// Direct-mapped hash table for INPUT key lookup.
	// 71 slots chosen so FNV-1a hash % 71 gives zero collisions for the 18 input keys.
	struct KeyHashTable
	{
		static const int SIZE = 71;
		int16_t slots[SIZE]; // Keys enum value, or -1 if empty
		bool built = false;

		void insert(uint32_t h, int value)
		{
			int idx = (int)(h % SIZE);
			if (slots[idx] != -1)
				throw std::runtime_error("JSON Parser: hash collision in key lookup table");
			slots[idx] = (int16_t)value;
		}

		int find(uint32_t h, const char *str, int len) const
		{
			int v = slots[(int)(h % SIZE)];
			if (v >= 0 && ((int)strlen(AIS::KeyMap[v][JSON_DICT_INPUT]) != len || memcmp(AIS::KeyMap[v][JSON_DICT_INPUT], str, len) != 0))
				return -1;
			return v;
		}
	};

	class Parser
	{
	private:
		int dict = 0;
		bool skipUnknownKeys = false;

		const char *p_start = nullptr;
		const char *p = nullptr;
		const char *pend = nullptr;

		enum class TokenType
		{
			LeftBrace,
			RightBrace,
			LeftBracket,
			RightBracket,
			Integer,
			FloatingPoint,
			Colon,
			Comma,
			True,
			False,
			Null,
			String,
			End
		};

		TokenType currentType = TokenType::End;
		const char *tokenStart = nullptr;
		const char *tokenEnd = nullptr;
		bool tokenEscaped = false;
		std::string escapedText;

		static uint32_t hashRange(const char *data, int len)
		{
			uint32_t h = 2166136261u;
			for (int i = 0; i < len; i++)
				h = (h ^ (unsigned char)data[i]) * 16777619u;
			return h;
		}

		void error(const std::string &err, int pos);

		// tokenizer
		void skip_whitespace();
		void next();

		// parser
		void error_parser(const std::string &err);
		bool is_match(TokenType t);
		void must_match(TokenType t, const std::string &err);
		int search();
		std::string tokenString() const;
		void parse_into_core(JSON *o, Pool *pool);
		Value parse_value(Pool *pool);
		void skip_value();

		static KeyHashTable keyLookup; // INPUT only

	public:
		static void buildKeyLookup()
		{
			if (keyLookup.built)
				return;

			for (int i = 0; i < KeyHashTable::SIZE; i++)
				keyLookup.slots[i] = -1;

			for (int i = 0; i < AIS::KEY_COUNT; i++)
				if (AIS::KeyMap[i][JSON_DICT_INPUT][0] != '\0')
				{
					const char *key = AIS::KeyMap[i][JSON_DICT_INPUT];
					keyLookup.insert(hashRange(key, strlen(key)), i);
				}

			keyLookup.built = true;
		}

		int linearSearch() const
		{
			const char *str = tokenEscaped ? escapedText.data() : tokenStart;
			int slen = tokenEscaped ? (int)escapedText.size() : (int)(tokenEnd - tokenStart);

			for (int i = 0; i < AIS::KEY_COUNT; i++)
			{
				const char *key = AIS::KeyMap[i][dict];
				if (key[0] != '\0' && (int)strlen(key) == slen && memcmp(key, str, slen) == 0)
					return i;
			}
			return -1;
		}

		void parse_into(JSON &target, Pool &pool, const std::string &j);

	public:
		Parser(int d = JSON_DICT_FULL) : dict(d) {}

		Document parse(const std::string &j);
		void parse_into(Document &doc, const std::string &j) { parse_into(doc.root, doc.pool, j); }
		void setSkipUnknown(bool b) { skipUnknownKeys = b; }
		void setMap(int d)
		{
			dict = d;
		}
	};
}
