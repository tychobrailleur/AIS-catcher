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
#include <cstring>
#include <cmath>

#include "Parser.h"
#include "Parse.h"

namespace JSON
{
	KeyHashTable Parser::keyLookup;

	// Parser -- Build JSON object from String

	void Parser::error(const std::string &err, int pos)
	{
		const int char_limit = 40;
		int sz = (int)(pend - json.data());
		int from = MAX(pos - char_limit, 0);
		int to = MIN(pos + char_limit, sz);

		std::stringstream ss;
		for (int i = from; i < to; i++)
		{
			char c = json[i];
			char d = (c == '\t' || c == '\r' || c == '\n') ? ' ' : c;
			ss << d;
		}
		ss << std::endl
		   << std::string(MIN(char_limit, pos), ' ') << "^" << std::endl
		   << std::string(MIN(char_limit, pos), ' ') << "^" << std::endl;
		throw std::runtime_error("syntax error in JSON: " + err);
	}

	// 64-bit scanning helpers

	// Detect if any byte in a 64-bit word equals the target byte
	static inline uint64_t has_byte(uint64_t word, char target)
	{
		uint64_t mask = 0x0101010101010101ULL * (unsigned char)target;
		uint64_t v = word ^ mask;
		return (v - 0x0101010101010101ULL) & ~v & 0x8080808080808080ULL;
	}


	// Count trailing zeros to find first flagged byte position
	static inline int first_byte_pos(uint64_t mask)
	{
#if defined(__GNUC__) || defined(__clang__)
		return __builtin_ctzll(mask) >> 3;
#elif defined(_MSC_VER)
		unsigned long idx;
		_BitScanForward64(&idx, mask);
		return (int)(idx >> 3);
#else
		// portable fallback
		int n = 0;
		while (!(mask & 0xFF))
		{
			mask >>= 8;
			n++;
		}
		return n;
#endif
	}

	// JIT Tokenizer - lexes one token per call

	void Parser::skip_whitespace()
	{
		while (p < pend && (unsigned char)*p <= ' ' && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
			p++;
	}

	void Parser::next()
	{
		skip_whitespace();

		if (p >= pend)
		{
			currentType = TokenType::End;
			tokenStart = tokenEnd = p;
			return;
		}

		char c = *p;

		// number
		if ((unsigned char)(c - '0') < 10 || c == '-')
		{
			bool floating = false;
			bool scientific = false;
			tokenStart = p;

			do
			{
				if (*p == '.')
				{
					if (floating || tokenStart == p || (unsigned char)(p[-1] - '0') >= 10)
						error("malformed number", (int)(p - json.data()));
					else
						floating = true;
				}
				else if ((*p == 'e' || *p == 'E') && !scientific)
				{
					if ((unsigned char)(p[-1] - '0') >= 10 && p[-1] != '.')
						error("malformed number", (int)(p - json.data()));

					scientific = floating = true;
					p++;
					if (p != pend && (*p == '+' || *p == '-'))
						p++;

					if (p == pend || (unsigned char)(*p - '0') >= 10)
						error("malformed number", (int)(p - json.data()));
				}

				p++;

			} while (p != pend && ((unsigned char)(*p - '0') < 10 || *p == '.' || *p == 'e' || *p == 'E'));

			tokenEnd = p;
			currentType = floating ? TokenType::FloatingPoint : TokenType::Integer;
		}
		// string
		else if (c == '\"')
		{
			p++;
			tokenStart = p;
			tokenEscaped = false;

			// 64-bit fast scan: find " or \ or control char
			while (p + 8 <= pend)
			{
				uint64_t chunk;
				memcpy(&chunk, p, 8);

				uint64_t special = has_byte(chunk, '"') | has_byte(chunk, '\\') | has_byte(chunk, '\n') | has_byte(chunk, '\r');
				if (special)
				{
					p += first_byte_pos(special);
					goto found_special;
				}
				p += 8;
			}

			// tail bytes
			while (p < pend && *p != '\"' && *p != '\\' && *p != '\n' && *p != '\r')
				p++;

		found_special:
			if (p < pend && *p == '\\')
				tokenEscaped = true;

			if (!tokenEscaped)
			{
				// fast path - no escapes, just record position
				tokenEnd = p;
				if (p == pend || *p != '\"')
					error("line ends in string literal", (int)(p - json.data()));
				p++;
			}
			else
			{
				// slow path - build escaped string
				escapedText.clear();
				escapedText.append(tokenStart, p - tokenStart);

				while (p != pend && *p != '\"' && *p != '\n' && *p != '\r')
				{
					char ch = *p;
					if (ch == '\\')
					{
						if (++p == pend)
							error("line ends in string literal escape sequence", (int)(p - json.data()));
						ch = *p;
						switch (ch)
						{
						case '\"':
							break;
						case '\\':
							break;
						case '/':
							break;
						case 'b':
							ch = '\b';
							break;
						case 'f':
							ch = '\f';
							break;
						case 'n':
							ch = '\n';
							break;
						case 'r':
							ch = '\r';
							break;
						case 't':
							ch = '\t';
							break;
						case 'u':
						{
							if (p + 4 >= pend)
								error("line ends in string literal unicode escape sequence", (int)(p - json.data()));
							std::string hex(p + 1, 4);
							for (int i = 0; i < 4; i++)
								if (!std::isxdigit(hex[i]))
									error("illegal unicode escape sequence", (int)(p - json.data()));
							ch = std::stoi(hex, nullptr, 16);
							p += 4;
							break;
						}
						default:
							error("illegal escape sequence " + std::to_string((int)(ch)), (int)(p - json.data()));
						}
					}
					escapedText += ch;
					p++;
				}

				tokenStart = nullptr;
				tokenEnd = nullptr;

				if (p == pend || *p != '\"')
					error("line ends in string literal", (int)(p - json.data()));
				p++;
			}

			currentType = TokenType::String;
		}
		// keyword
		else if ((unsigned char)(c | 0x20) >= 'a' && (unsigned char)(c | 0x20) <= 'z')
		{
			tokenStart = p;

			while (p != pend && (unsigned char)(*p | 0x20) >= 'a' && (unsigned char)(*p | 0x20) <= 'z')
				p++;

			tokenEnd = p;
			int len = (int)(tokenEnd - tokenStart);

			if (len == 4 && memcmp(tokenStart, "true", 4) == 0)
				currentType = TokenType::True;
			else if (len == 5 && memcmp(tokenStart, "false", 5) == 0)
				currentType = TokenType::False;
			else if (len == 4 && memcmp(tokenStart, "null", 4) == 0)
				currentType = TokenType::Null;
			else
				error("illegal identifier : \"" + std::string(tokenStart, len) + "\"", (int)(p - json.data()));
		}
		// special characters
		else
		{
			switch (c)
			{
			case '{':
				currentType = TokenType::LeftBrace;
				break;
			case '}':
				currentType = TokenType::RightBrace;
				break;
			case '[':
				currentType = TokenType::LeftBracket;
				break;
			case ']':
				currentType = TokenType::RightBracket;
				break;
			case ':':
				currentType = TokenType::Colon;
				break;
			case ',':
				currentType = TokenType::Comma;
				break;
			default:
				error("illegal character '" + std::string(1, c) + "'", (int)(p - json.data()));
				break;
			}
			tokenStart = p;
			tokenEnd = p;
			p++;
		}
	}

	// Parsing functions

	void Parser::error_parser(const std::string &err)
	{
		error(err, (int)(tokenStart - json.data()));
	}

	bool Parser::is_match(TokenType t)
	{
		return currentType == t;
	}

	void Parser::must_match(TokenType t, const std::string &err)
	{
		if (currentType != t)
			error_parser(err);
	}

	std::string Parser::tokenString() const
	{
		if (tokenEscaped)
			return escapedText;
		return std::string(tokenStart, tokenEnd - tokenStart);
	}

	int Parser::search()
	{
		if (dict == JSON_DICT_INPUT)
		{
			const char *str = tokenEscaped ? escapedText.data() : tokenStart;
			int len = tokenEscaped ? (int)escapedText.size() : (int)(tokenEnd - tokenStart);

			return keyLookup.find(hashRange(str, len), str, len);
		}

		return linearSearch();
	}

	void Parser::skip_value()
	{
		switch (currentType)
		{
		case TokenType::LeftBrace:
		{
			// skip nested object
			next();
			while (is_match(TokenType::String))
			{
				next(); // key
				must_match(TokenType::Colon, "expected ':'");
				next();
				skip_value();
				next();
				if (!is_match(TokenType::Comma))
					break;
				next();
			}
			must_match(TokenType::RightBrace, "expected '}'");
			break;
		}
		case TokenType::LeftBracket:
		{
			// skip array
			next();
			while (!is_match(TokenType::RightBracket))
			{
				skip_value();
				next();
				if (!is_match(TokenType::Comma))
					break;
				next();
			}
			must_match(TokenType::RightBracket, "expected ']'");
			break;
		}
		// all scalar types: already consumed by next()
		case TokenType::String:
		case TokenType::Integer:
		case TokenType::FloatingPoint:
		case TokenType::True:
		case TokenType::False:
		case TokenType::Null:
			break;
		default:
			error_parser("unexpected token while skipping value");
			break;
		}
	}

	Value Parser::parse_value(JSON *o)
	{
		Value v = Value();
		v.setNull();
		switch (currentType)
		{
		case TokenType::Integer:
			v.setInt(Util::Parse::Integer(std::string(tokenStart, tokenEnd - tokenStart)));
			break;
		case TokenType::FloatingPoint:
			v.setFloat(Util::Parse::Float(std::string(tokenStart, tokenEnd - tokenStart)));
			break;
		case TokenType::True:
			v.setBool(true);
			break;
		case TokenType::LeftBrace:
			o->objects.push_back(parse_core());
			v.setObject(o->objects.back().get());
			break;
		case TokenType::False:
			v.setBool(false);
			break;
		case TokenType::Null:
			v.setNull();
			break;
		case TokenType::String:
			v.setString(o->addString(tokenString()));
			break;
		case TokenType::LeftBracket:
		{
			std::vector<Value> *arr = o->addArray();

			next();

			while (!is_match(TokenType::RightBracket))
			{
				arr->push_back(parse_value(o));
				next();
				if (!is_match(TokenType::Comma))
					break;
				next();
				if (is_match(TokenType::RightBracket))
					error_parser("comma cannot be followed by ']'");
			}

			must_match(TokenType::RightBracket, "expected ']'");
			v.setArray(arr);
		}

			break;
		case TokenType::End:
			error_parser("unexpected end of file");

		default:
			error_parser("value parse not implemented");
			break;
		}
		return v;
	}

	std::shared_ptr<JSON> Parser::parse_core()
	{
		std::shared_ptr<JSON> o(new JSON());

		must_match(TokenType::LeftBrace, "expected '{'");
		next();

		while (is_match(TokenType::String))
		{
			int idx = search();
			if (idx < 0 && !skipUnknownKeys)
				error_parser("\"" + tokenString() + "\" is not an allowed \"key\"");

			next();
			must_match(TokenType::Colon, "expected \':\'");
			next();

			if (idx < 0)
				skip_value();
			else
				o->Add(idx, parse_value(o.get()));

			next();

			if (!is_match(TokenType::Comma))
				break;
			next();
			if (!is_match(TokenType::String))
				error_parser("comma needs to be followed by property");
		}

		must_match(TokenType::RightBrace, "expected '}'");
		return o;
	}

	void Parser::parse_into_core(JSON *o)
	{
		must_match(TokenType::LeftBrace, "expected '{'");
		next();

		while (is_match(TokenType::String))
		{
			int idx = search();
			if (idx < 0 && !skipUnknownKeys)
				error_parser("\"" + tokenString() + "\" is not an allowed \"key\"");

			next();
			must_match(TokenType::Colon, "expected \':\'");
			next();

			if (idx < 0)
				skip_value();
			else
				o->Add(idx, parse_value(o));

			next();

			if (!is_match(TokenType::Comma))
				break;
			next();
			if (!is_match(TokenType::String))
				error_parser("comma needs to be followed by property");
		}

		must_match(TokenType::RightBrace, "expected '}'");
	}

	std::shared_ptr<JSON> Parser::parse(const std::string &j)
	{
		json = j;
		p = json.data();
		pend = p + json.size();
		next();
		auto result = parse_core();
		next();
		must_match(TokenType::End, "expected END");

		return result;
	}

	void Parser::parse_into(JSON &target, const std::string &j)
	{
		json = j;
		p = json.data();
		pend = p + json.size();
		target.clear();
		next();
		parse_into_core(&target);
		next();
		must_match(TokenType::End, "expected END");
	}
}
