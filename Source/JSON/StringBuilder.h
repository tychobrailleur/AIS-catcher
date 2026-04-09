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

	void stringify(const char *str, size_t len, std::string &json, bool esc = true);

	inline void stringify(const std::string &str, std::string &json, bool esc = true)
	{
		stringify(str.data(), str.size(), json, esc);
	}

	inline std::string stringify(const std::string &str, bool esc = true)
	{
		std::string s;
		stringify(str.data(), str.size(), s, esc);
		return s;
	}

	// Direct-to-string unsigned formatter (no std::to_string temporary).
	// Only kept because DB::getPathGeoJSON's not-found fallback still
	// stitches a literal by hand. All other numeric formatting goes
	// through Writer's member methods.
	void append_uint(std::string &out, unsigned long long v);

	// Fast writer that owns raw ptr/end cursors into a std::string sink and
	// grows the backing string on demand. Hot path is a single-branch byte
	// write — same speed as a fixed char buffer, but with no truncation risk.
	// Provides both low-level primitives (append, append_int, append_string_escaped,
	// ...) and a fluent JSON object builder (kv, val, beginObject, endArray, ...).
	//
	// On destruction (or explicit finish()) the backing string is resized to
	// the actually-written content; capacity persists across calls when the
	// same target string is reused.
	class Writer
	{
		std::string *target;
		size_t start_off;
		char *ptr;
		char *end;
		bool need_sep = false;

		// Grow the backing string, preserving the write offset.
		void grow(size_t extra)
		{
			size_t used = (size_t)(ptr - &(*target)[0]);
			size_t cur = target->size();
			size_t need = used + extra;
			size_t nsz = cur * 2;
			if (nsz < need)
				nsz = need + 64;
			target->resize(nsz);
			ptr = &(*target)[0] + used;
			end = &(*target)[0] + nsz;
		}

		// ---- Unchecked write primitives.
		// Caller is responsible for ensuring enough capacity has been reserved.
		// Used to avoid per-byte capacity checks in the hot path: composite
		// methods reserve their worst case once, then call only put_* helpers. ----

		inline void put_char(char c) { *ptr++ = c; }

		inline void put_bytes(const char *s, size_t n)
		{
			memcpy(ptr, s, n);
			ptr += n;
		}

		// Up to 20 decimal digits. Caller must have reserved >= 20.
		inline void put_uint_raw(unsigned long long v)
		{
			char tmp[20];
			int len = 0;
			do
			{
				tmp[len++] = (char)('0' + (int)(v % 10));
				v /= 10;
			} while (v);
			for (int i = len - 1; i >= 0; i--)
				*ptr++ = tmp[i];
		}

		// Optional sign + up to 20 digits. Caller must have reserved >= 21.
		inline void put_int_raw(long long v)
		{
			unsigned long long uv;
			if (v < 0)
			{
				*ptr++ = '-';
				// `0ULL - (unsigned long long)v` is well-defined unsigned
				// wrap and produces |v| for all v, including LLONG_MIN
				// (where `v = -v` would be UB).
				uv = 0ULL - (unsigned long long)v;
			}
			else
			{
				uv = (unsigned long long)v;
			}
			put_uint_raw(uv);
		}

		// Optional sign + integer + '.' + 6 fractional digits.
		// Caller must have reserved >= 28 and pre-checked NaN.
		// Uses round-half-to-even (banker's rounding) to match printf %f
		// and std::to_string exactly.
		inline void put_float_raw(double v)
		{
			if (v < 0)
			{
				*ptr++ = '-';
				v = -v;
			}

			long long whole = (long long)v;
			double frac_d = (v - (double)whole) * 1000000.0;
			int frac = (int)(frac_d + 0.5);
			// If we landed exactly on a .5 case and rounded UP to an odd value,
			// snap back down so the result is even (banker's rounding).
			if ((double)frac - frac_d == 0.5 && (frac & 1))
				frac--;
			if (frac >= 1000000)
			{
				whole++;
				frac -= 1000000;
			}

			put_uint_raw((unsigned long long)whole);

			*ptr++ = '.';
			ptr[5] = (char)('0' + frac % 10);
			frac /= 10;
			ptr[4] = (char)('0' + frac % 10);
			frac /= 10;
			ptr[3] = (char)('0' + frac % 10);
			frac /= 10;
			ptr[2] = (char)('0' + frac % 10);
			frac /= 10;
			ptr[1] = (char)('0' + frac % 10);
			frac /= 10;
			ptr[0] = (char)('0' + frac);
			ptr += 6;
		}

		// Writes ',' if a separator is pending. Caller must have reserved >= 1.
		inline void put_sep_raw()
		{
			if (need_sep)
				*ptr++ = ',';
			need_sep = false;
		}

		// Writes "key":. Caller must have reserved >= klen + 3.
		inline void put_kvkey_raw(const char *k, size_t klen)
		{
			*ptr++ = '"';
			memcpy(ptr, k, klen);
			ptr += klen;
			*ptr++ = '"';
			*ptr++ = ':';
		}

		// JSON-escaped string body (no surrounding quotes).
		// Caller must have reserved >= len*6.
		void put_string_escaped_body_raw(const char *str, size_t len)
		{
			const char *s = str;
			const char *sEnd = s + len;
			const char *start = s;

			while (s < sEnd)
			{
				unsigned char c = (unsigned char)*s;
				if (c >= 0x20 && c != '"' && c != '\\')
				{
					s++;
					continue;
				}
				if (s > start)
				{
					size_t n = (size_t)(s - start);
					memcpy(ptr, start, n);
					ptr += n;
				}
				switch (c)
				{
				case '"':
					memcpy(ptr, "\\\"", 2);
					ptr += 2;
					break;
				case '\\':
					memcpy(ptr, "\\\\", 2);
					ptr += 2;
					break;
				case '\b':
					memcpy(ptr, "\\b", 2);
					ptr += 2;
					break;
				case '\f':
					memcpy(ptr, "\\f", 2);
					ptr += 2;
					break;
				case '\n':
					memcpy(ptr, "\\n", 2);
					ptr += 2;
					break;
				case '\r':
					memcpy(ptr, "\\r", 2);
					ptr += 2;
					break;
				case '\t':
					memcpy(ptr, "\\t", 2);
					ptr += 2;
					break;
				default:
				{
					static const char hex[] = "0123456789abcdef";
					ptr[0] = '\\';
					ptr[1] = 'u';
					ptr[2] = '0';
					ptr[3] = '0';
					ptr[4] = hex[(c >> 4) & 0xF];
					ptr[5] = hex[c & 0xF];
					ptr += 6;
					break;
				}
				}
				start = ++s;
			}
			if (s > start)
			{
				size_t n = (size_t)(s - start);
				memcpy(ptr, start, n);
				ptr += n;
			}
		}

		// JSON-escaped string with surrounding quotes.
		// Caller must have reserved >= len*6 + 2.
		void put_string_escaped_raw(const char *str, size_t len)
		{
			*ptr++ = '"';
			put_string_escaped_body_raw(str, len);
			*ptr++ = '"';
		}

	public:
		explicit Writer(std::string &dst, size_t headroom = 4096)
		{
			target = &dst;
			start_off = dst.size();
			if (dst.capacity() < start_off + headroom)
				dst.reserve(start_off + headroom);
			dst.resize(dst.capacity());
			ptr = &dst[0] + start_off;
			end = &dst[0] + dst.size();
		}

		~Writer() { finish(); }

		// Trim backing string down to written content. Idempotent.
		// Returns total bytes written since construction.
		int finish()
		{
			if (!target)
				return 0;
			size_t written = (size_t)(ptr - (&(*target)[0] + start_off));
			target->resize(start_off + written);
			target = nullptr;
			return (int)written;
		}

		inline void reserve_more(size_t n)
		{
			if ((size_t)(end - ptr) < n)
				grow(n);
		}

		// ---- Low-level primitives ----

		inline void append(char c)
		{
			if (ptr >= end)
				grow(1);
			*ptr++ = c;
		}

		inline void append(const char *s, size_t len)
		{
			reserve_more(len);
			put_bytes(s, len);
		}

		// Compile-time-length overload: preferred for string literals,
		// avoids the runtime strlen() call entirely.
		template <size_t N>
		inline void append_lit(const char (&s)[N]) { append(s, N - 1); }

		// Runtime overload: only used when length is genuinely unknown.
		inline void append_lit(const char *s) { append(s, strlen(s)); }

		void append_uint(unsigned long long v)
		{
			reserve_more(20);
			put_uint_raw(v);
		}

		void append_int(long long v)
		{
			reserve_more(21);
			put_int_raw(v);
		}

		void append_float(double v)
		{
			if (v != v)
			{
				reserve_more(4);
				put_bytes("null", 4);
				return;
			}
			reserve_more(28);
			put_float_raw(v);
		}

		void append_string_escaped(const char *str, size_t len)
		{
			// Worst case: 6x expansion for \uXXXX + 2 quotes
			reserve_more(len * 6 + 2);
			put_string_escaped_raw(str, len);
		}

		void append_string_escaped(const std::string &s)
		{
			append_string_escaped(s.data(), s.size());
		}

		// Returns pointer to the start of content written by this Writer.
		// Valid only until the next grow() / append. Useful for in-place
		// post-processing such as CRC computation over the just-written bytes.
		char *buffer_start() { return &(*target)[0] + start_off; }
		size_t written() const { return (size_t)(ptr - (&(*target)[0] + start_off)); }

		// ---- High-level JSON-builder methods (auto-separator) ----
		// Each reserves its worst case once, then writes via put_*_raw helpers.
		// Reservation budgets:
		//   sep:  1
		//   int:  21 (sign + 20 digits)
		//   uint: 20
		//   float:28 (sign + 20 whole + '.' + 6 frac); NaN is "null" = 4
		//   key:  klen + 3 ('"' + key + '":')
		//   esc:  n*6 + 2 (worst-case string escaping)

		Writer &beginObject()
		{
			reserve_more(2);
			put_sep_raw();
			put_char('{');
			return *this;
		}
		Writer &endObject()
		{
			reserve_more(1);
			put_char('}');
			need_sep = true;
			return *this;
		}
		Writer &beginArray()
		{
			reserve_more(2);
			put_sep_raw();
			put_char('[');
			return *this;
		}
		Writer &endArray()
		{
			reserve_more(1);
			put_char(']');
			need_sep = true;
			return *this;
		}

		Writer &val(int v)
		{
			reserve_more(22);
			put_sep_raw();
			put_int_raw((long long)v);
			need_sep = true;
			return *this;
		}
		Writer &val(long v)
		{
			reserve_more(22);
			put_sep_raw();
			put_int_raw((long long)v);
			need_sep = true;
			return *this;
		}
		Writer &val(long long v)
		{
			reserve_more(22);
			put_sep_raw();
			put_int_raw(v);
			need_sep = true;
			return *this;
		}
		Writer &val(unsigned v)
		{
			reserve_more(21);
			put_sep_raw();
			put_uint_raw((unsigned long long)v);
			need_sep = true;
			return *this;
		}
		Writer &val(unsigned long v)
		{
			reserve_more(21);
			put_sep_raw();
			put_uint_raw((unsigned long long)v);
			need_sep = true;
			return *this;
		}
		Writer &val(unsigned long long v)
		{
			reserve_more(21);
			put_sep_raw();
			put_uint_raw(v);
			need_sep = true;
			return *this;
		}
		Writer &val(double v)
		{
			if (v != v)
			{
				reserve_more(5);
				put_sep_raw();
				put_bytes("null", 4);
				need_sep = true;
				return *this;
			}
			reserve_more(29);
			put_sep_raw();
			put_float_raw(v);
			need_sep = true;
			return *this;
		}
		// Exact-match for float so callers don't need to write (double)x.
		// Without this overload float would reach val(double) via Floating
		// Promotion — works, but the explicit overload removes the noise.
		Writer &val(float v) { return val((double)v); }
		Writer &val(bool v)
		{
			reserve_more(6); // sep + "false"
			put_sep_raw();
			if (v)
				put_bytes("true", 4);
			else
				put_bytes("false", 5);
			need_sep = true;
			return *this;
		}
		Writer &val(const char *s, size_t n)
		{
			reserve_more(1 + n * 6 + 2);
			put_sep_raw();
			put_string_escaped_raw(s, n);
			need_sep = true;
			return *this;
		}
		// Writes one JSON string value built from two pieces, no temporary
		// allocation. Both pieces are escaped into the same "..." literal.
		Writer &val(const char *s1, size_t n1, const char *s2, size_t n2)
		{
			reserve_more(1 + (n1 + n2) * 6 + 2);
			put_sep_raw();
			*ptr++ = '"';
			put_string_escaped_body_raw(s1, n1);
			put_string_escaped_body_raw(s2, n2);
			*ptr++ = '"';
			need_sep = true;
			return *this;
		}
		// Fixed-size null-terminated buffer overload. Captures the array
		// size at compile time and uses strnlen(s, N-1) — bounded, safe
		// against missing terminators, and elides the explicit length arg
		// at call sites: w.val(ship.shipname) instead of
		// w.val(ship.shipname, strlen(ship.shipname)).
		template <size_t N>
		Writer &val(const char (&s)[N])
		{
			return val(s, strnlen(s, N - 1));
		}
		template <size_t N>
		Writer &val(const char (&s)[N], const char *suffix, size_t suffix_n)
		{
			return val(s, strnlen(s, N - 1), suffix, suffix_n);
		}
		Writer &val_null()
		{
			reserve_more(5);
			put_sep_raw();
			put_bytes("null", 4);
			need_sep = true;
			return *this;
		}
		// Writes val(v) unless v equals sentinel, in which case writes null.
		// Two type params so e.g. char vs. int sentinels deduce independently.
		template <typename T, typename U>
		Writer &val_unless(T v, U sentinel)
		{
			return v == sentinel ? val_null() : val(v);
		}

		Writer &kv(const char *k, int v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 25); // sep + key + sign + 20 digits
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_int_raw((long long)v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, long v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 25);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_int_raw((long long)v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, long long v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 25);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_int_raw(v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, unsigned v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 24); // sep + key + 20 digits
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_uint_raw((unsigned long long)v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, unsigned long v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 24);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_uint_raw((unsigned long long)v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, unsigned long long v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 24);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_uint_raw(v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, double v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 32); // sep + key + sign + 20 + '.' + 6
			put_sep_raw();
			put_kvkey_raw(k, klen);
			if (v != v)
				put_bytes("null", 4);
			else
				put_float_raw(v);
			need_sep = true;
			return *this;
		}
		Writer &kv(const char *k, float v) { return kv(k, (double)v); }
		Writer &kv(const char *k, const char *s, size_t n)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 4 + 1 + n * 6 + 2); // key + sep + escaped value
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_string_escaped_raw(s, n);
			need_sep = true;
			return *this;
		}
		// Two-piece string value, no temporary allocation. Both pieces are
		// escaped into the same "..." literal.
		Writer &kv(const char *k, const char *s1, size_t n1, const char *s2, size_t n2)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 4 + 1 + (n1 + n2) * 6 + 2);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			*ptr++ = '"';
			put_string_escaped_body_raw(s1, n1);
			put_string_escaped_body_raw(s2, n2);
			*ptr++ = '"';
			need_sep = true;
			return *this;
		}
		// Fixed-size null-terminated buffer overloads — see val() above.
		template <size_t N>
		Writer &kv(const char *k, const char (&s)[N])
		{
			return kv(k, s, strnlen(s, N - 1));
		}
		template <size_t N>
		Writer &kv(const char *k, const char (&s)[N], const char *suffix, size_t suffix_n)
		{
			return kv(k, s, strnlen(s, N - 1), suffix, suffix_n);
		}
		Writer &kv(const char *k, const std::string &v)
		{
			return kv(k, v.data(), v.size());
		}
		Writer &kv(const char *k, const char *v)
		{
			return kv(k, v, strlen(v));
		}
		Writer &kv(const char *k, bool v)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 9); // sep + key + "false"
			put_sep_raw();
			put_kvkey_raw(k, klen);
			if (v)
				put_bytes("true", 4);
			else
				put_bytes("false", 5);
			need_sep = true;
			return *this;
		}
		Writer &kv_null(const char *k)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 8); // sep + key + 'null'
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_bytes("null", 4);
			need_sep = true;
			return *this;
		}
		// Writes kv(k, v) unless v equals sentinel, in which case writes
		// kv_null(k). Mirror of val_unless for keyed objects.
		template <typename T, typename U>
		Writer &kv_unless(const char *k, T v, U sentinel)
		{
			return v == sentinel ? kv_null(k) : kv(k, v);
		}

		// Writes "key":raw  — caller-supplied raw must be valid JSON.
		Writer &kv_raw(const char *k, const char *raw, size_t rawlen)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 4 + rawlen);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			put_bytes(raw, rawlen);
			need_sep = true;
			return *this;
		}
		Writer &kv_raw(const char *k, const std::string &raw)
		{
			return kv_raw(k, raw.data(), raw.size());
		}

		// Writes "key": and leaves cursor positioned for the next val/raw write.
		// need_sep is cleared so the following val() does not emit a comma.
		Writer &key(const char *k)
		{
			size_t klen = strlen(k);
			reserve_more(klen + 4);
			put_sep_raw();
			put_kvkey_raw(k, klen);
			need_sep = false;
			return *this;
		}

		// Like val() but for already-serialised JSON fragments. Honors need_sep.
		Writer &raw_val(const char *raw, size_t rawlen)
		{
			reserve_more(1 + rawlen);
			put_sep_raw();
			put_bytes(raw, rawlen);
			need_sep = true;
			return *this;
		}
		Writer &raw_val(const std::string &raw)
		{
			return raw_val(raw.data(), raw.size());
		}

		// Escape hatch for pre-built JSON fragments (no escaping, no separator)
		Writer &raw(const char *s)
		{
			append_lit(s);
			return *this;
		}
		Writer &raw(const std::string &s)
		{
			append(s.data(), s.size());
			return *this;
		}
	};

	// Serializes a JSON object using a Writer sink. Stateless w.r.t. the
	// destination — each stringify() call constructs a Writer.
	class Serializer
	{
	private:
		int dict = 0;
		bool stringify_enhanced = false;

		// All three helpers below rely on Writer's internal need_sep flag
		// for comma placement — no manual ',' bookkeeping. Each entry point
		// assumes need_sep reflects the surrounding JSON context: beginArray /
		// beginObject / key() will emit a leading comma iff one is due, and
		// the primitive val() / kv() calls leave need_sep=true on exit.

		void write_value(const Value &v, Writer &w)
		{
			switch (v.getType())
			{
			case Value::Type::STRING:
			{
				const std::string &s = v.getStringRef();
				w.val(s.data(), s.size());
				break;
			}
			case Value::Type::INT:
				w.val((long long)v.getInt());
				break;
			case Value::Type::FLOAT:
				w.val(v.getFloat());
				break;
			case Value::Type::BOOL:
				w.val(v.getBool());
				break;
			case Value::Type::OBJECT:
				write_object(v.getObject(), w);
				break;
			case Value::Type::ARRAY_STRING:
			{
				const std::vector<std::string> &as = v.getStringArray();
				w.beginArray();
				for (const std::string &s : as)
					w.val(s.data(), s.size());
				w.endArray();
				break;
			}
			case Value::Type::ARRAY:
			{
				const std::vector<Value> &a = v.getArray();
				w.beginArray();
				for (const Value &e : a)
					write_value(e, w);
				w.endArray();
				break;
			}
			default:
				w.val_null();
				break;
			}
		}

		void write_value_enhanced(const Value &v, int key_index, Writer &w)
		{
			w.beginObject().key("value");
			write_value(v, w);

			if (key_index >= 0 && key_index < AIS::KEY_COUNT)
			{
				const AIS::KeyInfo &info = AIS::KeyInfoMap[key_index];

				if (info.unit && info.unit[0] != '\0')
					w.kv("unit", info.unit);
				if (info.description && info.description[0] != '\0')
					w.kv("description", info.description);
				if (info.lookup_table && (v.isInt() || v.isFloat()))
				{
					int nv = v.isInt() ? v.getInt() : static_cast<int>(v.getFloat());
					if (nv >= 0 && nv < (int)info.lookup_table->size())
						w.kv("text", (*info.lookup_table)[nv]);
				}
			}
			w.endObject();
		}

		void write_object(const JSON &object, Writer &w)
		{
			w.beginObject();
			for (const Member &p : object.getMembers())
			{
				if (p.Key() < 0 || p.Key() >= AIS::KEY_COUNT)
					continue;
				const char *key = AIS::KeyMap[p.Key()][dict];
				if (key[0] == '\0')
					continue;

				w.key(key);

				if (stringify_enhanced)
					write_value_enhanced(p.Get(), p.Key(), w);
				else
					write_value(p.Get(), w);
			}
			w.endObject();
		}

	public:
		Serializer(int d = JSON_DICT_FULL) : dict(d) {}

		// Appends the serialized object (plus optional suffix) to dst.
		// Returns number of bytes appended.
		int stringify(const JSON &object, std::string &dst, const char *suffix = nullptr)
		{
			Writer w(dst);
			write_object(object, w);
			if (suffix)
				w.append_lit(suffix);
			return w.finish();
		}

		// Serializes object into an existing Writer. Honors the Writer's current
		// separator state, so callers can interleave serialized objects with
		// other Writer operations (e.g. inside a beginArray()/endArray() frame).
		void stringify(const JSON &object, Writer &w)
		{
			write_object(object, w);
		}

		void setMap(int d) { dict = d; }
		void setStringifyEnhanced(bool enhanced) { stringify_enhanced = enhanced; }
		bool getStringifyEnhanced() const { return stringify_enhanced; }
	};
}
