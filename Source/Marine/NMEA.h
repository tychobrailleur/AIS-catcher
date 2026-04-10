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
		ParseState state = ParseState::IDLE;
		std::string line;
		int count = 0;

		// Scan context — set by Receive(), used by scanners
		const char *buf = nullptr;
		int bufsize = 0;
		int pos = 0;
		// Per-message context — set by scanners/process functions, read by dispatch
		struct MsgCtx {
			int64_t rxtime = 0;
			int station = -1;
			int groupId = 0;
			uint64_t ssc = 0;
			uint16_t sl = 0;
			int64_t toa = 0;

			void reset() { rxtime = 0; station = -1; groupId = 0; ssc = 0; sl = 0; toa = 0; }
		} mctx;

		void findStart();
		void scanLine(TAG &tag);
		void scanJSON(TAG &tag);
		void scanBinary(TAG &tag);
		int own_mmsi = -1;

		std::vector<AIVDM> queue;

		void initMsg(char channel, int src);
		void dispatchAIS(TAG &tag);
		void addline(const AIVDM &a);
		void reset();
		void clean(char, int, int groupId = 0);
		int search();

		static bool isHEX(char c) { unsigned u = (unsigned char)c; return (u - '0' < 10u) | ((u | 0x20) - 'a' < 6u); }
		static int fromHEX(char c) { unsigned u = (unsigned char)c, d = u - '0'; return d < 10u ? d : (int)((u | 0x20) - 'a' + 10); }

		float GpsToDecimal(const char *, int len, char, bool &error);

		bool cfg_regenerate = false;
		bool cfg_stamp = true;
		bool cfg_crc_check = false;
		bool cfg_JSON_input = false;
		bool cfg_VDO = true;
		bool cfg_warnings = true;
		bool cfg_GPS = true;

		JSON::Parser parser;
		JSON::Document jsonDoc;

		void split(const std::string &, size_t offset = 0);
		void processJSONsentence(TAG &tag);
		bool processAIS(const std::string &s, TAG &tag);
		bool processGGA(const std::string &s, TAG &tag);
		bool processGLL(const std::string &s, TAG &tag);
		bool processRMC(const std::string &s, TAG &tag);
		bool processBinaryPacket(TAG &tag);
		bool parseTagBlock(const std::string &s, std::string &nmea);
		bool processTagBlock(const std::string &s, TAG &tag);
		bool processNMEAline(const std::string &s, TAG &tag);

	public:
		NMEA() : parser(JSON_DICT_INPUT)
		{
			parser.setSkipUnknown(true);
		}

		virtual ~NMEA() {}
		void Receive(const RAW *data, int len, TAG &tag);

		void setRegenerate(bool b) { cfg_regenerate = b; }
		bool getRegenerate() { return cfg_regenerate; }

		void setVDO(bool b) { cfg_VDO = b; }
		bool getVDO() { return cfg_VDO; }
		void setUUID(const std::string &u) { uuid = u; }
		const std::string &getUUID() { return uuid; }

		void setStation(int s) { station = s; }
		int getStation() { return station; }

		void setWarnings(bool b) { cfg_warnings = b; }
		void setGPS(bool b) { cfg_GPS = b; }
		void setCRCcheck(bool b) { cfg_crc_check = b; }
		bool getCRCcheck() { return cfg_crc_check; }
		void setJSON(bool b) { cfg_JSON_input = b; }
		void setStamp(bool b) { cfg_stamp = b; }
		bool getStamp() { return cfg_stamp; }
		void setOwnMMSI(int m) { own_mmsi = m; }

		Connection<GPS> outGPS;
	};
}
