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
#include <cstdio>

#include "Common.h"
#include "JSON.h"
#include "Keys.h"

namespace JSON
{

	void stringify(const std::string &str, std::string &json, bool esc = true);

	inline std::string stringify(const std::string &str, bool esc = true)
	{
		std::string s;
		stringify(str, s, esc);
		return s;
	}

	class StringBuilder
	{
	private:
		int dict = 0;
		bool stringify_enhanced = false;

		char *buf = nullptr;
		char *ptr = nullptr;
		char *end = nullptr;

		void append(char c)
		{
			if (ptr < end)
				*ptr++ = c;
		}

		void append(const char *s)
		{
			while (*s && ptr < end)
				*ptr++ = *s++;
		}

		void append(const char *s, int len)
		{
			int avail = end - ptr;
			int n = len < avail ? len : avail;
			memcpy(ptr, s, n);
			ptr += n;
		}

		void append(const std::string &s)
		{
			append(s.data(), s.size());
		}

		void append_uint(unsigned long long v)
		{
			char tmp[20];
			int len = 0;
			do
			{
				tmp[len++] = '0' + (int)(v % 10);
				v /= 10;
			} while (v);

			if (ptr + len <= end)
				for (int i = len - 1; i >= 0; i--)
					*ptr++ = tmp[i];
		}

		void append_int(long int v)
		{
			if (v < 0)
			{
				append('-');
				v = -v;
			}
			append_uint((unsigned long long)v);
		}

		void append_float(double v)
		{
			if (v != v)
			{
				append("null", 4);
				return;
			}

			if (v < 0)
			{
				append('-');
				v = -v;
			}

			long long whole = (long long)v;
			int frac = (int)((v - (double)whole) * 1000000.0 + 0.5);

			if (frac >= 1000000)
			{
				whole++;
				frac -= 1000000;
			}

			append_uint((unsigned long long)whole);

			if (ptr + 7 <= end)
			{
				*ptr++ = '.';
				ptr[5] = '0' + frac % 10;
				frac /= 10;
				ptr[4] = '0' + frac % 10;
				frac /= 10;
				ptr[3] = '0' + frac % 10;
				frac /= 10;
				ptr[2] = '0' + frac % 10;
				frac /= 10;
				ptr[1] = '0' + frac % 10;
				frac /= 10;
				ptr[0] = '0' + frac;
				ptr += 6;
			}
		}

		void append_string_escaped(const std::string &str)
		{
			const char *s = str.data();
			int len = (int)str.size();

			append('"');
			const char *start = s;
			const char *sEnd = s + len;
			while (s < sEnd)
			{
				char c = *s;
				if (c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\0')
				{
					if (s > start)
						append(start, (int)(s - start));
					if (c == '"')
						append("\\\"", 2);
					else if (c == '\\')
						append("\\\\", 2);
					else if (c == '\n')
						append("\\n", 2);
					// \r and \0 are silently dropped
					start = ++s;
				}
				else
				{
					s++;
				}
			}
			if (s > start)
				append(start, (int)(s - start));
			append('"');
		}

		void write_value(const Value &v)
		{
			switch (v.getType())
			{
			case Value::Type::STRING:
				append_string_escaped(v.getStringRef());
				break;
			case Value::Type::INT:
				append_int(v.getInt());
				break;
			case Value::Type::FLOAT:
				append_float(v.getFloat());
				break;
			case Value::Type::BOOL:
				if (v.getBool())
					append("true", 4);
				else
					append("false", 5);
				break;
			case Value::Type::OBJECT:
				write_object(v.getObject());
				break;
			case Value::Type::ARRAY_STRING:
			{
				const std::vector<std::string> &as = v.getStringArray();
				append('[');
				for (int i = 0; i < (int)as.size(); i++)
				{
					if (i > 0)
						append(',');
					append_string_escaped(as[i]);
				}
				append(']');
				break;
			}
			case Value::Type::ARRAY:
			{
				const std::vector<Value> &a = v.getArray();
				append('[');
				for (int i = 0; i < (int)a.size(); i++)
				{
					if (i > 0)
						append(',');
					write_value(a[i]);
				}
				append(']');
				break;
			}
			default:
				append("null", 4);
				break;
			}
		}

		void write_value_enhanced(const Value &v, int key_index)
		{
			append('{');
			append("\"value\":", 8);
			write_value(v);

			if (key_index >= 0 && key_index < AIS::KEY_COUNT)
			{
				const AIS::KeyInfo &info = AIS::KeyInfoMap[key_index];

				if (info.unit && info.unit[0] != '\0')
				{
					append(",\"unit\":", 8);
					append_string_escaped(info.unit);
				}

				if (info.description && info.description[0] != '\0')
				{
					append(",\"description\":", 15);
					append_string_escaped(info.description);
				}

				if (info.lookup_table && (v.isInt() || v.isFloat()))
				{
					int numeric_value = v.isInt() ? v.getInt() : static_cast<int>(v.getFloat());
					if (numeric_value >= 0 && numeric_value < (int)info.lookup_table->size())
					{
						append(",\"text\":", 8);
						append_string_escaped((*info.lookup_table)[numeric_value]);
					}
				}
			}

			append('}');
		}

		void write_object(const JSON &object)
		{
			bool first = true;
			append('{');
			for (const Property &p : object.getProperties())
			{
				if (p.Key() < 0 || p.Key() >= AIS::KEY_COUNT)
					continue;

				const char* key = AIS::KeyMap[p.Key()][dict];
				if (key[0] == '\0')
					continue;

				if (!first)
					append(',');
				first = false;

				append('"');
				append(key);
				append("\":", 2);

				if (stringify_enhanced)
					write_value_enhanced(p.Get(), p.Key());
				else
					write_value(p.Get());
			}
			append('}');
		}

	public:
		StringBuilder(int d = JSON_DICT_FULL) : dict(d) {}

		int stringify(const JSON &object, char *buffer, int length, const char *suffix = nullptr)
		{
			buf = buffer;
			ptr = buffer;
			end = buffer + length - 1;

			write_object(object);

			if (suffix)
				append(suffix);

			*ptr = '\0';
			return ptr - buf;
		}

		void setMap(int d) { dict = d; }
		void setStringifyEnhanced(bool enhanced) { stringify_enhanced = enhanced; }
		bool getStringifyEnhanced() const { return stringify_enhanced; }
	};
}
