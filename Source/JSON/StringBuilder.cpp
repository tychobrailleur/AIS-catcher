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

#include <string>

#include "StringBuilder.h"

namespace JSON {

	void stringify(const std::string& str, std::string& json, bool esc) {
		if (esc) json += '\"';
		const char *s = str.data();
		size_t len = str.size();
		size_t start = 0;

		for (size_t i = 0; i < len; i++) {
			char c = s[i];
			if (c == '\"' || c == '\\' || c == '\n' || c == '\r' || c == '\0') {
				if (i > start) json.append(s + start, i - start);
				if (c == '\"') json.append("\\\"", 2);
				else if (c == '\\') json.append("\\\\", 2);
				else if (c == '\n') json.append("\\n", 2);
				// \r and \0 silently dropped
				start = i + 1;
			}
		}
		if (len > start) json.append(s + start, len - start);
		if (esc) json += '\"';
	}
}
