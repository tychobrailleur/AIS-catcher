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

#include <iomanip>
#include <cstdint>

#include "Message.h"
#include "Stream.h"

#include "JSON/JSON.h"
#include "Parser.h"
#include "StringBuilder.h"
#include "Keys.h"

namespace AIS
{

	class NMEA : public SimpleStreamInOut<RAW, Message>
	{
		Message msg;
		int station = 0;
		std::string uuid;

		enum class ParseState
		{
			IDLE,
			JSON,
			NMEA,
			BINARY,
			TAG_BLOCK
		};

		struct AIVDM
		{
			std::string sentence;
			uint16_t data_offset = 0;
			uint16_t data_len = 0;

			uint64_t timestamp = 0;
			int groupId = 0; // NMEA 4.0 tag block group ID

			void reset()
			{
				sentence.clear();
				data_offset = 0;
				data_len = 0;
				timestamp = time(nullptr);
				groupId = 0;
				message_error = 0;
			}
			char channel = 0;
			int count = 0;
			int number = 0;
			int ID = 0;
			int checksum = 0;
			int fillbits = 0;
			int talkerID = 0;
			uint32_t message_error;
		} aivdm;

		// Zero-allocation field splitter: stores delimiter positions into source string
		const std::string *splitStr = nullptr;
		int splitDelim[18] = {}; // delimiter positions (leading sentinel + up to 16 commas + trailing sentinel)
		int splitCount = 0; // number of fields
		int splitChecksum = 0; // XOR checksum accumulated during split

		int partLen(int i) const { return splitDelim[i + 1] - splitDelim[i] - 1; }
		char partAt(int i, int j) const { return (*splitStr)[splitDelim[i] + 1 + j]; }
		const char *partPtr(int i) const { return splitStr->data() + splitDelim[i] + 1; }
		bool partEmpty(int i) const { return partLen(i) == 0; }
		std::string partStr(int i) const { return splitStr->substr(splitDelim[i] + 1, partLen(i)); }
		int partInt(int i) const;
		std::string trimPart(int i) const;

		char prev = '\n';
		ParseState state = ParseState::IDLE;
		std::string line;
		bool hasStar = false;
		int count = 0;
		int own_mmsi = -1;

		std::vector<AIVDM> queue;

		void submitAIS(TAG &tag, int64_t t, uint64_t ssc, uint16_t sl, int thisstation, int64_t toa = 0);
		void addline(const AIVDM &a);
		void reset(char);
		void clean(char, int, int groupId = 0);
		int search(const AIVDM &a);

		bool isNMEAchar(char c) { return (c >= 40 && c < 88) || (c >= 96 && c <= 56 + 0x3F); }
		bool isHEX(char c) { return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f'); }
		int fromHEX(char c) { return (c >= '0' && c <= '9') ? (c - '0') : ((c >= 'A' && c <= 'F') ? (c - 'A' + 10) : (c - 'a' + 10)); }

		int NMEAchecksum(const std::string &s);

		float GpsToDecimal(const char *, char, bool &error);

		bool regenerate = false;
		bool stamp = true;
		bool crc_check = false;
		bool JSON_input = false;
		bool VDO = true;
		bool warnings = true;
		bool includeGPS = true;

		JSON::Parser parser;
		JSON::Document jsonDoc;

		void split(const std::string &, size_t offset = 0);
		void processJSONsentence(const std::string &s, TAG &tag, int64_t t);
		bool processAIS(const std::string &s, TAG &tag, int64_t t, uint64_t ssc, uint16_t sl, int thisstation, int groupId, std::string &error_msg, int64_t toa = 0);
		bool processGGA(const std::string &s, TAG &tag, int64_t t, std::string &error_msg);
		bool processGLL(const std::string &s, TAG &tag, int64_t t, std::string &error_msg);
		bool processRMC(const std::string &s, TAG &tag, int64_t t, std::string &error_msg);
		bool processBinaryPacket(const std::string &packet, TAG &tag, std::string &error_msg);
		bool parseTagBlock(const std::string &s, std::string &nmea, int64_t &timestamp, int &thisstation, int &groupId, std::string &error_msg);
		bool processTagBlock(const std::string &s, TAG &tag, int64_t &t, std::string &error_msg);
		bool processNMEAline(const std::string &s, TAG &tag, int64_t t, int thisstation, int groupId, std::string &error_msg);
		bool isCompleteNMEA(const std::string &s, size_t offset, bool newline);

	public:
		NMEA() : parser(JSON_DICT_INPUT)
		{
			parser.setSkipUnknown(true);
		}

		virtual ~NMEA() {}
		void Receive(const RAW *data, int len, TAG &tag);

		void setRegenerate(bool b) { regenerate = b; }
		bool getRegenerate() { return regenerate; }

		void setVDO(bool b) { VDO = b; }
		bool getVDO() { return VDO; }
		void setUUID(const std::string &u) { uuid = u; }
		const std::string &getUUID() { return uuid; }

		void setStation(int s) { station = s; }
		int getStation() { return station; }

		void setWarnings(bool b) { warnings = b; }
		void setGPS(bool b) { includeGPS = b; }
		void setCRCcheck(bool b) { crc_check = b; }
		bool getCRCcheck() { return crc_check; }
		void setJSON(bool b) { JSON_input = b; }
		void setStamp(bool b) { stamp = b; }
		bool getStamp() { return stamp; }
		void setOwnMMSI(int m) { own_mmsi = m; }

		Connection<GPS> outGPS;
	};
}
