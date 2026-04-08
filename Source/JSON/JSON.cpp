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
#include <cmath>

#include "JSON.h"

namespace JSON
{

	void Value::to_string(std::string &str) const
	{
		switch (type)
		{
		case Value::Type::STRING:
			str += *data.s;
			break;
		case Value::Type::BOOL:
			if (data.b)
				str.append("true", 4);
			else
				str.append("false", 5);
			break;
		case Value::Type::INT:
			str += std::to_string(data.i);
			break;
		case Value::Type::FLOAT:
			str += std::to_string(data.f);
			break;
		case Value::Type::EMPTY:
			str.append("null", 4);
			break;
		case Value::Type::OBJECT:
			str.append("object", 6);
			break;
		case Value::Type::ARRAY_STRING:
		case Value::Type::ARRAY:
			str.append("array", 5);
			break;
		default:
			str.append("error", 5);
			break;
		}
	}
}
