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

#include "NMEA.h"
#include "Parse.h"
#include "Convert.h"
#include "Helper.h"
#include <cmath>

namespace AIS
{

	const std::string empty;

	void NMEA::reset(char c)
	{
		state = ParseState::IDLE;
		line.clear();
		hasStar = false;
		prev = c;
	}

	void NMEA::clean(char c, int t, int groupId)
	{
		auto i = queue.begin();
		uint64_t now = time(nullptr);
		while (i != queue.end())
		{
			// Match by groupId if available, otherwise by channel+talkerID
			bool match = (groupId != 0 && i->groupId == groupId) ||
						 (groupId == 0 && i->channel == c && i->talkerID == t);
			if (match || (i->timestamp + 3 < now))
				i = queue.erase(i);
			else
				i++;
		}
	}

	int NMEA::search(const AIVDM &a)
	{
		// multiline message, find previous lines with matching group
		// return: 0 = Not Found, -1: Found but inconsistent with input, >0: number of previous message
		int lastNumber = 0;
		for (auto it = queue.rbegin(); it != queue.rend(); it++)
		{
			// Match by groupId if available, otherwise by channel+talkerID
			bool match = (aivdm.groupId != 0 && it->groupId == aivdm.groupId) ||
						 (aivdm.groupId == 0 && it->channel == aivdm.channel && it->talkerID == aivdm.talkerID);
			if (match)
			{
				if (it->count != aivdm.count || it->ID != aivdm.ID)
					lastNumber = -1;
				else
					lastNumber = it->number;
				break;
			}
		}
		return lastNumber;
	}

	int NMEA::NMEAchecksum(const std::string &s)
	{
		int c = 0;
		if (s.length() > 4) // Need at least "$X*XX" format
		{
			for (size_t i = 1; i < s.length() - 3; i++)
				c ^= s[i];
		}
		return c;
	}

	void NMEA::submitAIS(TAG &tag, int64_t t, uint64_t ssc, uint16_t sl, int thisstation, int64_t toa)
	{
		bool checksum_error = aivdm.checksum != splitChecksum;

		if (checksum_error)
		{
			if (warnings)
				Warning() << "NMEA: incorrect checksum [" << aivdm.sentence << "] from station " << (thisstation == -1 ? station : thisstation) << ".";

			if (crc_check)
				return;

			aivdm.message_error |= MESSAGE_ERROR_NMEA_CHECKSUM;
		}

		if (aivdm.count == 1)
		{
			tag.error = aivdm.message_error;

			msg.clear();
			if (stamp || t == 0)
				msg.Stamp();
			else
				msg.setRxTimeMicros(t);
			msg.setTOA(toa);
			msg.setOrigin(aivdm.channel, thisstation == -1 ? station : thisstation, own_mmsi);
			msg.setStartIdx(ssc);
			msg.setEndIdx(ssc + sl);

			addline(aivdm);

			if (msg.validate())
			{
				if (regenerate)
					msg.buildNMEA(tag);
				else
					msg.NMEA.push_back(std::move(aivdm.sentence));

				Send(&msg, 1, tag);
			}
			else if (msg.getLength() > 0)

				if (warnings)
					Warning() << "NMEA: invalid message of type " << msg.type() << " and length " << msg.getLength() << " from station " << (thisstation == -1 ? station : thisstation) << ".";

			return;
		}

		int result = search(aivdm);

		if (aivdm.number != result + 1 || result == -1)
		{
			clean(aivdm.channel, aivdm.talkerID, aivdm.groupId);
			if (aivdm.number != 1)
			{
				return;
			}
		}

		queue.push_back(aivdm);
		if (aivdm.number != aivdm.count)
			return;

		// multiline messages are now complete and in the right order
		// we create a message and add the payloads to it
		msg.clear();
		if (stamp || t == 0)
			msg.Stamp();
		else
			msg.setRxTimeMicros(t);
		msg.setTOA(toa);
		msg.setOrigin(aivdm.channel, thisstation == -1 ? station : thisstation, own_mmsi);

		for (auto it = queue.begin(); it != queue.end(); it++)
		{
			// Match by groupId if available, otherwise by channel+talkerID
			bool match = (aivdm.groupId != 0 && it->groupId == aivdm.groupId) ||
						 (aivdm.groupId == 0 && it->channel == aivdm.channel && it->talkerID == aivdm.talkerID);
			if (match && it->count == aivdm.count && it->ID == aivdm.ID)
			{
				tag.error |= it->message_error;

				addline(*it);
				if (!regenerate)
					msg.NMEA.push_back(it->sentence);
			}
		}

		if (msg.validate())
		{
			if (regenerate)
				msg.buildNMEA(tag, aivdm.ID);

			Send(&msg, 1, tag);
		}
		else if (warnings)
			Warning() << "NMEA: invalid message of type " << msg.type() << " and length " << msg.getLength();

		clean(aivdm.channel, aivdm.talkerID, aivdm.groupId);
	}

	void NMEA::addline(const AIVDM &a)
	{
		msg.appendPayload(a.sentence.data() + a.data_offset, a.data_len);
		if (a.count == a.number)
			msg.reduceLength(a.fillbits);
	}

	// SWAR helpers
	static constexpr size_t SWAR_ONES = ~(size_t)0 / 255;
	static constexpr size_t SWAR_HIGHS = SWAR_ONES * 128;

	static constexpr size_t swar_mask(char target)
	{
		return SWAR_ONES * (unsigned char)target;
	}

	static inline size_t has_byte(size_t word, size_t mask)
	{
		size_t v = word ^ mask;
		return (v - SWAR_ONES) & ~v & SWAR_HIGHS;
	}

	void NMEA::split(const std::string &s, size_t offset /*= 0*/)
	{
		splitStr = &s;
		splitCount = 0;
		splitDelim[0] = (int)offset - 1; // sentinel before first field

		const char *ptr = s.data() + offset + 1; // +1 to skip leading $/!
		const char *end = s.data() + s.size();

		// Phase 1: scan size_t words for '*' to find checksum boundary
		// and accumulate XOR checksum, while recording comma positions
		size_t cs_accum = 0;
		const char *p = ptr;
		constexpr size_t mask_star = swar_mask('*');
		constexpr size_t mask_comma = swar_mask(',');

		// Process size_t-aligned chunks
		while (p + sizeof(size_t) <= end && splitCount < 16)
		{
			size_t word;
			memcpy(&word, p, sizeof(size_t));

			// Check for '*' in this word
			if (has_byte(word, mask_star))
				break;

			// Accumulate checksum (all bytes before '*')
			cs_accum ^= word;

			// Check for commas in this word
			if (has_byte(word, mask_comma))
			{
				for (size_t j = 0; j < sizeof(size_t) && splitCount < 16; j++)
				{
					if (p[j] == ',')
						splitDelim[++splitCount] = (int)(p - s.data() + j);
				}
			}

			p += sizeof(size_t);
		}

		// Fold the size_t XOR accumulator down to a single byte
		int cs = 0;
		for (size_t k = 0; k < sizeof(size_t); k++)
			cs ^= (int)((cs_accum >> (k * 8)) & 0xFF);

		// Byte-at-a-time tail (remainder + everything after the break)
		bool past_star = false;
		for (; p < end && splitCount < 16; p++)
		{
			char c = *p;
			if (c == '*') { past_star = true; continue; }
			if (!past_star) cs ^= c;
			if (c == ',')
				splitDelim[++splitCount] = (int)(p - s.data());
		}

		splitChecksum = cs;
		splitDelim[++splitCount] = (int)s.size(); // sentinel after last field
	}

	int NMEA::partInt(int i) const
	{
		int val = 0;
		bool neg = false;
		for (int j = 0, len = partLen(i); j < len; j++)
		{
			char c = partAt(i, j);
			if (j == 0 && c == '-') { neg = true; continue; }
			if (c < '0' || c > '9') break;
			val = val * 10 + (c - '0');
		}
		return neg ? -val : val;
	}

	std::string NMEA::trimPart(int i) const
	{
		std::string r;
		for (int j = 0, len = partLen(i); j < len; j++)
		{
			char c = partAt(i, j);
			if (c != ' ') r += c;
		}
		return r;
	}

	// stackoverflow variant (need to find the reference)
	float NMEA::GpsToDecimal(const char *nmeaPos, char quadrant, bool &error)
	{
		float v = 0;
		if (nmeaPos && strlen(nmeaPos) > 5)
		{
			char integerPart[3 + 1];
			int digitCount = (nmeaPos[4] == '.' ? 2 : 3);
			memcpy(integerPart, nmeaPos, digitCount);
			integerPart[digitCount] = 0;
			nmeaPos += digitCount;

			int degrees = atoi(integerPart);
			if (degrees == 0 && integerPart[0] != '0')
			{
				error |= true;
				return 0;
			}

			float minutes = (float)atof(nmeaPos);
			if (minutes == 0 && nmeaPos[0] != '0')
			{
				error |= true;
				return 0;
			}

			v = degrees + minutes / 60.0;
			if (quadrant == 'W' || quadrant == 'S')
				v = -v;
		}
		return v;
	}

	bool NMEA::processGGA(const std::string &s, TAG &tag, int64_t t, std::string &error_msg)
	{

		if (!includeGPS)
			return true;

		bool error = false;

		split(s);

		if (splitCount != 15)
		{
			error_msg = "NMEA: GPGGA does not have 15 parts but " + std::to_string(splitCount);
			return false;
		}

		int checksum = partLen(14) > 2 ? (fromHEX(partAt(14, partLen(14) - 2)) << 4) | fromHEX(partAt(14, partLen(14) - 1)) : -1;

		if (checksum != NMEAchecksum(line))
		{
			if (crc_check)
			{

				error_msg = "NMEA: incorrect checksum.";
				return false;
			}
		}

		// no proper fix
		int fix = partInt(6);
		if (fix != 1 && fix != 2)
		{
			error_msg = "NMEA: no fix in GPGGA NMEA:" + partStr(6);
			return false;
		}

		std::string lat_coord = trimPart(2);
		std::string lat_quad = trimPart(3);
		std::string lon_coord = trimPart(4);
		std::string lon_quad = trimPart(5);

		if (lat_quad.empty() || lon_quad.empty())
		{
			return false;
		}

		GPS gps(GpsToDecimal(lat_coord.c_str(), lat_quad[0], error),
				GpsToDecimal(lon_coord.c_str(), lon_quad[0], error),
				s, empty);

		if (error)
		{
			error_msg = "NMEA: error in GPGGA coordinates.";
			return false;
		}

		outGPS.Send(&gps, 1, tag);

		return true;
	}

	bool NMEA::processRMC(const std::string &s, TAG &tag, int64_t t, std::string &error_msg)
	{

		if (!includeGPS)
			return true;

		bool error = false;

		split(s);

		if (splitCount < 12 || splitCount > 14)
			return false;

		int last = splitCount - 1;
		int checksum = partLen(last) > 2 ? (fromHEX(partAt(last, partLen(last) - 2)) << 4) | fromHEX(partAt(last, partLen(last) - 1)) : -1;

		if (checksum != NMEAchecksum(line))
		{
			if (crc_check)
			{
				error_msg = "incorrect checksum";
				return false;
			}
		}

		std::string lat_quad = trimPart(4); // N/S
		std::string lon_quad = trimPart(6); // E/W

		if (lat_quad.empty() || lon_quad.empty())
		{
			error_msg = "NMEA: no coordinates in RMC";
			return false;
		}

		GPS gps(GpsToDecimal(trimPart(3).c_str(), lat_quad[0], error), // lat
				GpsToDecimal(trimPart(5).c_str(), lon_quad[0], error), // lon
				s, empty);

		if (error)
		{
			error_msg = "NMEA: error in RMC coordinates.";
			return false;
		}

		outGPS.Send(&gps, 1, tag);

		return true;
	}

	bool NMEA::processGLL(const std::string &s, TAG &tag, int64_t t, std::string &error_msg)
	{

		if (!includeGPS)
			return true;

		bool error = false;
		split(s);

		if (splitCount != 8)
		{
			error_msg = "NMEA: GLL does not have 8 parts but " + std::to_string(splitCount);
			return false;
		}

		int checksum = partLen(7) > 2 ? (fromHEX(partAt(7, partLen(7) - 2)) << 4) | fromHEX(partAt(7, partLen(7) - 1)) : -1;

		if (checksum != NMEAchecksum(line))
		{
			if (warnings && !crc_check)
				Warning() << "NMEA: incorrect checksum [" << line << "].";

			if (crc_check)
			{
				error_msg = "NMEA: incorrect checksum";
				return false;
			}
		}

		if (partEmpty(2) || partEmpty(4))
		{
			return false;
		}

		float lat = GpsToDecimal(partPtr(1), partAt(2, 0), error);
		float lon = GpsToDecimal(partPtr(3), partAt(4, 0), error);

		if (error)
		{
			error_msg = "NMEA: error in GLL coordinates.";
			return false;
		}

		GPS gps(lat, lon, s, empty);
		outGPS.Send(&gps, 1, tag);

		return true;
	}

	bool NMEA::processAIS(const std::string &str, TAG &tag, int64_t t, uint64_t ssc, uint16_t sl, int thisstation, int groupId, std::string &error_msg, int64_t toa)
	{
		// Fast path: most NMEA lines start with $ or !
		size_t pos = (!str.empty() && (str[0] == '$' || str[0] == '!')) ? 0 : str.find_first_of("$!");
		if (pos == std::string::npos)
		{
			error_msg = "NMEA: no $ or ! in AIS sentence";
			return false;
		}

		size_t len = str.size() - pos;
		bool isNMEA = len > 10 && (str[pos + 3] == 'V' && str[pos + 4] == 'D' && (str[pos + 5] == 'M' || str[pos + 5] == 'O'));
		if (!isNMEA)
		{
			return true; // no NMEA -> ignore
		}

		split(str, pos);
		aivdm.reset();

		if (splitCount != 7 || partLen(0) != 6 || partLen(1) != 1 || partLen(2) != 1 || partLen(3) > 1 || partLen(4) > 1 || partLen(6) != 4)
		{
			error_msg = "NMEA: AIS sentence does not have 7 parts or has invalid part sizes";
			return false;
		}
		if (partAt(0, 0) != '$' && partAt(0, 0) != '!')
		{
			error_msg = "NMEA: AIS sentence does not start with $ or !";
			return false;
		}

		char t1 = partAt(0, 1), t2 = partAt(0, 2);
		if (!(t1 >= 'A' && t1 <= 'Z') || (!(t2 >= 'A' && t2 <= 'Z') && !(t2 >= '0' && t2 <= '9')))
		{
			error_msg = "NMEA: AIS sentence does not have valid talker ID";
			return false;
		}

		if (partAt(0, 3) != 'V' || partAt(0, 4) != 'D' || (partAt(0, 5) != 'M' && partAt(0, 5) != 'O'))
		{
			error_msg = "NMEA: AIS sentence does not have valid VDM or VDO";
			return false;
		}

		aivdm.talkerID = ((partAt(0, 1) << 8) | partAt(0, 2));
		aivdm.count = partAt(1, 0) - '0';
		aivdm.number = partAt(2, 0) - '0';
		aivdm.ID = partLen(3) > 0 ? partAt(3, 0) - '0' : 0;
		aivdm.channel = partLen(4) > 0 ? partAt(4, 0) : '?';

		aivdm.data_offset = splitDelim[5] + 1;
		aivdm.data_len = partLen(5);
		aivdm.fillbits = partAt(6, 0) - '0';

		if (!isHEX(partAt(6, 2)) || !isHEX(partAt(6, 3)))
		{
			error_msg = "NMEA: AIS sentence does not have valid checksum";
			return false;
		}
		aivdm.checksum = (fromHEX(partAt(6, 2)) << 4) | fromHEX(partAt(6, 3));

		// One assignment, reuses sentence buffer if large enough
		aivdm.sentence.assign(str, pos, std::string::npos);
		aivdm.data_offset -= pos;
		aivdm.groupId = groupId;

		submitAIS(tag, t, ssc, sl, thisstation, toa);

		return true;
	}

	void NMEA::processJSONsentence(const std::string &s, TAG &tag, int64_t t)
	{

		if (s[0] == '{')
		{
			try
			{
				// JSON::Parser parser(&AIS::KeyMap, JSON_DICT_FULL);
				// parser.setSkipUnknown(true);
				parser.parse_into(jsonDoc, s);

				std::string cls = "";
				std::string dev = "";
				std::string suuid = "";
				std::string message = "";
				int thisstation = -1;
				uint64_t ssc = 0;
				uint16_t sl = 0;
				int64_t toa = 0;

				t = 0;

				// phase 1, get the meta data in place
				for (const auto &p : jsonDoc.getMembers())
				{
					switch (p.Key())
					{
					case AIS::KEY_CLASS:
						cls = p.Get().getString();
						break;
					case AIS::KEY_UUID:
						suuid = p.Get().getString();
						break;
					case AIS::KEY_DEVICE:
						dev = p.Get().getString();
						break;
					case AIS::KEY_SIGNAL_POWER:
					case AIS::KEY_DBM:
						tag.level = p.Get().getFloat(LEVEL_UNDEFINED);
						break;
					case AIS::KEY_PPM:
						tag.ppm = p.Get().getFloat(PPM_UNDEFINED);
						break;
					case AIS::KEY_RXUXTIME:
					{
						double ts = p.Get().getFloat();
						t = (int64_t)std::llround(ts * 1000000.0);
					}
					break;
					case AIS::KEY_TOA:
					{
						double ts = p.Get().getFloat();
						toa = (int64_t)std::llround(ts * 1000000.0);
					}
					break;
					case AIS::KEY_STATION_ID:
						thisstation = p.Get().getInt();
						break;
					case AIS::KEY_VERSION:
						tag.version = p.Get().getInt();
						break;
					case AIS::KEY_HARDWARE:
						tag.hardware = p.Get().getString();
						break;
					case AIS::KEY_DRIVER:
						tag.driver = (Type)p.Get().getInt();
						break;
					case AIS::KEY_SAMPLE_START_COUNT:
						ssc = p.Get().getInt();
						break;
					case AIS::KEY_SAMPLE_LENGTH:
						sl = p.Get().getInt();
						break;
					case AIS::KEY_IPV4:
						tag.ipv4 = p.Get().getInt();
						break;
					case AIS::KEY_MESSAGE:
						message = p.Get().getString();
						break;
					}
				}

				if (cls == "AIS" && (dev == "AIS-catcher" || dev == "dAISy-catcher") && (uuid.empty() || suuid == uuid))
				{
					if (dev == "dAISy-catcher" && toa != 0 && !stamp)
						t = toa;

					for (const auto &p : jsonDoc.getMembers())
					{
						if (p.Key() == AIS::KEY_NMEA)
						{
							if (p.Get().isArray())
							{
								for (const auto &v : p.Get().getArray())
								{
									std::string error;
									const std::string &line = v.getString();

									if (!processAIS(line, tag, t, ssc, sl, thisstation, 0, error, toa))
									{
										Warning() << "NMEA [" << (tag.ipv4 ? (Util::Convert::IPV4toString(tag.ipv4) + " - ") : "") << thisstation << "] " << error << " (" << line << ")";
									}
								}
							}
						}
					}
				}

				if ((dev == "AIS-catcher" || dev == "dAISy-catcher") && !message.empty())
				{
					if (cls == "error")
					{

						Error() << "[" << Util::Parse::DeviceTypeString(tag.driver) << "]: " << message;
					}
					else if (cls == "warning")
					{
						Warning() << "[" << Util::Parse::DeviceTypeString(tag.driver) << "]: " << message;
					}
					else
					{
						Info() << "[" << Util::Parse::DeviceTypeString(tag.driver) << "]: " << message;
					}
				}

				if (cls == "TPV" && includeGPS)
				{
					float lat = 0, lon = 0;

					for (const auto &p : jsonDoc.getMembers())
					{
						if (p.Key() == AIS::KEY_LAT)
						{
							if (p.Get().isFloat())
							{
								lat = p.Get().getFloat();
							}
							else if (p.Get().isInt())
							{
								lat = (float)p.Get().getInt();
							}
						}
						else if (p.Key() == AIS::KEY_LON)
						{
							if (p.Get().isFloat())
							{
								lon = p.Get().getFloat();
							}
							else if (p.Get().isInt())
							{
								lon = (float)p.Get().getInt();
							}
						}
					}

					if (lat != 0 || lon != 0)
					{
						GPS gps(lat, lon, empty, s);
						outGPS.Send(&gps, 1, tag);
					}
				}
			}
			catch (std::exception const &e)
			{
				Error() << "NMEA model: " << e.what();
			}
		}
	}

	bool NMEA::processBinaryPacket(const std::string &packet, TAG &tag, std::string &error_msg)
	{
		try
		{
			size_t idx = 0;
			auto getByte = [&]() -> uint8_t
			{
				if (idx >= packet.length())
					throw std::runtime_error("packet too short");

				uint8_t byte = packet[idx++];

				if (byte == 0xad)
				{
					if (idx >= packet.length())
						throw std::runtime_error("packet too short");

					uint8_t next = packet[idx++];

					if (next == 0xae)
						return '\n';
					if (next == 0xaf)
						return '\r';
					if (next == 0xad)
						return 0xad;
					throw std::runtime_error("invalid escape sequence");
				}
				return byte;
			};

			if (getByte() != 0xac)
				throw std::runtime_error("invalid magic byte");
			if (getByte() != 0x00)
				throw std::runtime_error("unsupported version");

			uint8_t flags = getByte();

			long long timestamp = 0;

			for (int i = 0; i < 8; i++)
				timestamp = (timestamp << 8) | getByte();

			if (flags & 0x01)
			{
				tag.level = (int16_t)((getByte() << 8) | getByte()) / 10.0f;
				tag.ppm = (int8_t)getByte() / 10.0f;
			}

			char channel = getByte();
			int length_bits = (getByte() << 8) | getByte();

			if (length_bits < 0 || length_bits > MAX_AIS_LENGTH)
			{
				throw std::runtime_error("invalid message length: " + std::to_string(length_bits));
			}

			msg.clear();
			msg.setRxTimeUnix(timestamp);
			msg.setOrigin(channel, station, own_mmsi);
			msg.setStartIdx(0);
			msg.setEndIdx(0);

			for (int i = 0; i < (length_bits + 7) / 8; i++)
				msg.setUint(i * 8, 8, getByte());

			msg.setLength(length_bits);

			if (flags & 0x02)
			{
				uint16_t calc_crc = Util::Helper::CRC16((const uint8_t *)packet.data(), idx);
				uint16_t recv_crc = (getByte() << 8) | getByte();
				if (recv_crc != calc_crc)
				{
					if (crc_check)
						throw std::runtime_error("CRC mismatch");
					if (warnings)
						Warning() << "NMEA: binary packet CRC mismatch";
				}
			}

			if (!msg.validate())
			{
				error_msg = "message validation failed";
				return false;
			}
			msg.buildNMEA(tag);
			Send(&msg, 1, tag);
			return true;
		}
		catch (const std::exception &e)
		{
			error_msg = e.what();
			return false;
		}
	}

	bool NMEA::parseTagBlock(const std::string &s, std::string &nmea, int64_t &timestamp, int &thisstation, int &groupId, std::string &error_msg)
	{
		// Format: \s:source,c:timestamp,g:seq-total-id*checksum\!AIVDM,...
		// Find the second backslash that ends the tag block
		size_t tagEnd = s.find('\\', 1);
		if (tagEnd == std::string::npos)
		{
			error_msg = "tag block: no closing backslash";
			return false;
		}

		std::string tagBlock = s.substr(1, tagEnd - 1); // Extract content between backslashes
		nmea = s.substr(tagEnd + 1);					// NMEA sentence after tag block

		if (nmea.empty())
		{
			error_msg = "tag block: no NMEA sentence after tag block";
			return false;
		}

		// Remove checksum from tag block if present
		size_t checksumPos = tagBlock.find('*');
		if (checksumPos != std::string::npos)
		{
			// Verify checksum - need at least 2 hex chars after '*'
			int expectedChecksum = 0;
			if (checksumPos + 2 < tagBlock.size())
			{
				expectedChecksum = (fromHEX(tagBlock[checksumPos + 1]) << 4) | fromHEX(tagBlock[checksumPos + 2]);
			}
			int actualChecksum = 0;
			for (size_t i = 0; i < checksumPos; i++)
				actualChecksum ^= tagBlock[i];

			if (expectedChecksum != actualChecksum && crc_check)
			{
				error_msg = "tag block: checksum mismatch";
				return false;
			}
			tagBlock.resize(checksumPos);
		}

		// Parse individual fields
		std::stringstream ss(tagBlock);
		std::string field;
		while (std::getline(ss, field, ','))
		{
			if (field.size() < 2 || field[1] != ':')
				continue;

			char key = field[0];
			std::string value = field.substr(2);

			switch (key)
			{
			case 's': // Source - if starts with 's' followed by digits, extract station ID
				if (!value.empty() && value[0] == 's')
				{
					try
					{
						thisstation = std::stoi(value.substr(1));
					}
					catch (...)
					{
					}
				}
				break;
			case 'c': // Unix timestamp
				try
				{
					if (value.find('.') != std::string::npos)
					{
						double ts = std::stod(value);
						timestamp = (int64_t)std::llround(ts * 1000000.0);
					}
					else
					{
						int64_t raw = std::stoll(value);
						timestamp = (raw > 100000000000LL || raw < -100000000000LL) ? raw : (raw * 1000000);
					}
				}
				catch (...)
				{
				}
				break;
			case 'g': // Group: seq-total-groupId (e.g., "1-2-1234")
			{
				size_t dash1 = value.find('-');
				size_t dash2 = value.find('-', dash1 + 1);
				if (dash1 != std::string::npos && dash2 != std::string::npos)
				{
					try
					{
						groupId = std::stoi(value.substr(dash2 + 1));
					}
					catch (...)
					{
					}
				}
			}
			break;
			}
		}

		return true;
	}

	bool NMEA::isCompleteNMEA(const std::string &s, size_t offset, bool newline)
	{
		size_t len = s.size() - offset;
		if (len < 7)
			return false;

		// Check for VDM/VDO with valid checksum pattern
		bool isVDx = len > 10 && (s[offset + 3] == 'V' && s[offset + 4] == 'D' && (s[offset + 5] == 'M' || s[offset + 5] == 'O'));
		if (isVDx)
		{
			size_t sz = s.size();
			bool hasChecksum = isHEX(s[sz - 1]) && isHEX(s[sz - 2]) && s[sz - 3] == '*' &&
							   ((isdigit(s[sz - 4]) && s[sz - 5] == ',') || (s[sz - 4] == ','));
			if (hasChecksum)
				return true;
		}

		// For other NMEA types or incomplete VDM/VDO, require newline
		return newline;
	}

	bool NMEA::processNMEAline(const std::string &s, TAG &tag, int64_t t, int thisstation, int groupId, std::string &error_msg)
	{
		if (s.size() <= 5)
			return true;

		if (s[3] == 'V' && s[4] == 'D' && s[5] == 'M')
			return processAIS(s, tag, t, 0, 0, thisstation, groupId, error_msg);
		if (s[3] == 'V' && s[4] == 'D' && s[5] == 'O' && VDO)
			return processAIS(s, tag, t, 0, 0, thisstation, groupId, error_msg);
		if (s[3] == 'G' && s[4] == 'G' && s[5] == 'A')
			return processGGA(s, tag, t, error_msg);
		if (s[3] == 'R' && s[4] == 'M' && s[5] == 'C')
			return processRMC(s, tag, t, error_msg);
		if (s[3] == 'G' && s[4] == 'L' && s[5] == 'L')
			return processGLL(s, tag, t, error_msg);

		return true; // Unknown type, ignore
	}

	bool NMEA::processTagBlock(const std::string &s, TAG &tag, int64_t &t, std::string &error_msg)
	{
		std::string nmea;
		int thisstation = -1;
		int groupId = 0;

		if (!parseTagBlock(s, nmea, t, thisstation, groupId, error_msg))
			return false;

		return processNMEAline(nmea, tag, t, thisstation, groupId, error_msg);
	}

	// continue collection of full NMEA line in `sentence` and store location of commas in 'locs'
	void NMEA::Receive(const RAW *data, int len, TAG &tag)
	{
		try
		{
			int64_t t = 0;

			for (int j = 0; j < len; j++)
			{
				const char *buf = (const char *)data[j].data;
				int size = data[j].size;
				int i = 0;

				// Compile-time SWAR masks for Receive scanning
				constexpr size_t m_dollar = swar_mask('$'), m_bang = swar_mask('!');
				constexpr size_t m_lbrace = swar_mask('{'), m_bslash = swar_mask('\\'), m_0xac = swar_mask((char)0xac);
				constexpr size_t m_star = swar_mask('*'), m_cr = swar_mask('\r'), m_lf = swar_mask('\n');
				constexpr size_t m_tab = swar_mask('\t'), m_nul = swar_mask('\0'), m_rbrace = swar_mask('}');

				while (i < size)
				{
					if (state == ParseState::IDLE)
					{
						// SWAR fast-forward: skip bytes that can't be start characters
						// Start chars: $ (0x24), ! (0x21), { (0x7B), \ (0x5C), 0xAC
						// In IDLE after a newline, most bytes are \r\n between messages — skip them fast
						while (i + (int)sizeof(size_t) <= size)
						{
							size_t word;
							memcpy(&word, buf + i, sizeof(size_t));
							if (has_byte(word, m_dollar) || has_byte(word, m_bang) || has_byte(word, m_lbrace) || has_byte(word, m_bslash) || has_byte(word, m_0xac))
								break;
							// Update prev to last byte of this word
							prev = buf[i + sizeof(size_t) - 1];
							i += sizeof(size_t);
						}
						for (; i < size; i++)
						{
							char c = buf[i];
							if (c == '{' && (prev == '\n' || prev == '\r' || prev == '}'))
							{
								state = ParseState::JSON;
								line = c;
								count = 1;
								prev = c;
								i++;
								break;
							}
							else if (c == '\\' && (prev == '\n' || prev == '\r'))
							{
								state = ParseState::TAG_BLOCK;
								line = c;
								prev = c;
								i++;
								break;
							}
							else if (c == '$' || c == '!')
							{
								state = ParseState::NMEA;
								line = c;
								prev = c;
								i++;
								break;
							}
							else if ((unsigned char)c == 0xac)
							{
								state = ParseState::BINARY;
								line = c;
								prev = c;
								i++;
								break;
							}
							prev = c;
						}
						continue;
					}

					char c = buf[i];
					bool newline = (state == ParseState::BINARY) ? (c == '\n') : (c == '\r' || c == '\n' || c == '\t' || c == '\0');

					// Bulk-append block of non-special characters
					if (!newline && c != '{' && c != '}' && c != '*' && !hasStar)
					{
						int start = i;

						if (state == ParseState::NMEA || state == ParseState::TAG_BLOCK)
						{
							// SWAR scan: find first *, \r, \n, \t, \0 in 8-byte chunks
							// All target bytes are < 0x2A (*=0x2A), so check for any byte <= 0x2A
							while (i + (int)sizeof(size_t) <= size)
							{
								size_t word;
								memcpy(&word, buf + i, sizeof(size_t));
								// Check for '*' (0x2A) or any control char (< 0x20)
								if (has_byte(word, m_star) || has_byte(word, m_cr) || has_byte(word, m_lf) || has_byte(word, m_tab) || has_byte(word, m_nul))
									break;
								i += sizeof(size_t);
							}
							for (; i < size; i++)
							{
								char bc = buf[i];
								if (bc == '*' || bc == '\r' || bc == '\n' || bc == '\t' || bc == '\0')
									break;
							}
						}
						else if (state == ParseState::JSON)
						{
							while (i + (int)sizeof(size_t) <= size)
							{
								size_t word;
								memcpy(&word, buf + i, sizeof(size_t));
								if (has_byte(word, m_lbrace) || has_byte(word, m_rbrace) || has_byte(word, m_cr) || has_byte(word, m_lf) || has_byte(word, m_tab) || has_byte(word, m_nul))
									break;
								i += sizeof(size_t);
							}
							for (; i < size; i++)
							{
								char bc = buf[i];
								if (bc == '{' || bc == '}' || bc == '\r' || bc == '\n' || bc == '\t' || bc == '\0')
									break;
							}
						}
						else if (state == ParseState::BINARY)
						{
							while (i + (int)sizeof(size_t) <= size)
							{
								size_t word;
								memcpy(&word, buf + i, sizeof(size_t));
								if (has_byte(word, m_lf))
									break;
								i += sizeof(size_t);
							}
							for (; i < size; i++)
							{
								if (buf[i] == '\n')
									break;
							}
						}

						if (i > start)
						{
							line.append(buf + start, i - start);
							prev = buf[i - 1];
						}
						continue;
					}

					// Process single control/boundary character
					i++;

					if (!newline)
					{
						line += c;
						if (c == '*') hasStar = true;
					}
					prev = c;

					if (state == ParseState::JSON)
					{
						if (c == '{')
							count++;
						else if (c == '}')
						{
							--count;
							if (!count)
							{
								t = 0;
								tag.clear();
								processJSONsentence(line, tag, t);
								reset(c);
							}
						}
						else if (newline)
						{
							if (warnings)
								Warning() << "NMEA: newline in uncompleted JSON input not allowed";
							reset(c);
						}
					}
					else if (state == ParseState::NMEA)
					{
						if ((hasStar || newline) && isCompleteNMEA(line, 0, newline))
						{
							std::string error = "unspecified error";
							tag.clear();
							t = 0;

							if (!processNMEAline(line, tag, t, -1, 0, error))
							{
								if (warnings)
								{
									Warning() << "NMEA: error processing NMEA line " << line;
									Warning() << "NMEA [" << error << " (" << line << ")";
								}
							}
							reset(c);
						}
					}
					else if (state == ParseState::BINARY)
					{
						if (newline)
						{
							std::string error;
							tag.clear();
							if (!processBinaryPacket(line, tag, error))
							{
								if (warnings)
								{
									std::stringstream ss;
									ss << "NMEA: error processing binary packet: " << error
									   << " (length: " << line.length() << " bytes)";
									Warning() << ss.str();
								}
							}
							reset(c);
						}
					}
					else if (state == ParseState::TAG_BLOCK)
					{
						size_t tagEnd = line.find('\\', 1);
						size_t offset = (tagEnd != std::string::npos) ? (tagEnd + 1) : line.size();
						if ((hasStar || newline) && isCompleteNMEA(line, offset, newline))
						{
							std::string error;
							tag.clear();
							t = 0;

							if (!processTagBlock(line, tag, t, error))
							{
								if (warnings)
									Warning() << "NMEA: " << error;
							}
							reset(c);
						}
					}

					if (line.size() > 1024)
						reset(c);
				}
			}
		}
		catch (std::exception &e)
		{
			if (warnings)
				Warning() << "NMEA Receive: " << e.what();
		}
	}
}
