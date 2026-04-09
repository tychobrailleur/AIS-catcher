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

#include "AIS-catcher.h"
#include "DBMS/PostgreSQL.h"

namespace IO
{

#ifdef HASPSQL
	void PostgreSQL::post()
	{

		if (PQstatus(con) != CONNECTION_OK)
		{
			Warning() << "DBMS: Connection to PostgreSQL lost. Attempting to reset...";
			PQreset(con);

			if (PQstatus(con) != CONNECTION_OK)
			{
				Error() << "DBMS: Could not reset connection. Aborting post.";
				conn_fails++;
				return;
			}
			else
			{
				Warning() << "DBMS: Connection successfully reset.";
				conn_fails = 0;
			}
		}

		{
			const std::lock_guard<std::mutex> lock(queue_mutex);
			sql_trans = "DO $$\nDECLARE\n\tm_id INTEGER;\nBEGIN\n" + sql.str() + "\nEND $$;\n";
			sql.str("");
		}

		PGresult *res;
		res = PQexec(con, sql_trans.c_str());

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			Error() << "DBMS: Error writing PostgreSQL: " << PQerrorMessage(con);
			conn_fails = 1;
		}

		PQclear(res);
	}
#endif
	PostgreSQL::~PostgreSQL()
	{
#ifdef HASPSQL
		if (running)
		{

			running = false;
			terminate = true;
			run_thread.join();

			Debug() << "DBMS: stop thread and database closed.";
		}
		if (con != nullptr)
			PQfinish(con);
#endif
	}

#ifdef HASPSQL

	void PostgreSQL::process()
	{

		while (!terminate)
		{

			for (int i = 0; !terminate && i < (conn_fails == 0 ? INTERVAL : 2) && sql.tellp() < 32768 * 16; i++)
			{
				SleepSystem(1000);
			}

			if (sql.tellp())
				post();

			if (terminate)
				break;

			if (MAX_FAILS < 1000 && conn_fails > MAX_FAILS)
			{
				Error() << "DBMS: max attempts reached to connect to DBMS. Terminating.";
				StopRequest();
			}
		}
	}
#endif

	void PostgreSQL::setup()
	{
#ifdef HASPSQL

		db_keys.resize(AIS::KEY_COUNT, -1);
		Debug() << "Connecting to ProgreSQL database: \"" + conn_string + "\"\n";
		con = PQconnectdb(conn_string.c_str());

		if (con == nullptr || PQstatus(con) != CONNECTION_OK)
			throw std::runtime_error("DBMS: cannot open database :" + std::string(PQerrorMessage(con)));

		conn_fails = 0;

		PGresult *res = PQexec(con, "SELECT key_id, key_str FROM ais_keys");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			throw std::runtime_error("DBMS: error fetching ais_keys table: " + std::string(PQerrorMessage(con)));
		}

		int key_count = 0;
		for (int row = 0; row < PQntuples(res); row++)
		{
			int id = atoi(PQgetvalue(res, row, 0));
			std::string name = PQgetvalue(res, row, 1);
			bool found = false;
			for (int i = 0; i < db_keys.size(); i++)
			{
				if (AIS::KeyMap[i][JSON_DICT_FULL] == name)
				{
					db_keys[i] = id;
					found = true;
					key_count++;
					break;
				}
			}
			if (!found)
				throw std::runtime_error("DBMS: The requested key \"" + name + "\" in ais_keys is not defined.");
		}

		if (key_count > 0 && !MSGS)
		{
			Info() << "DBMS: no messages logged in combination with property logging. MSGS ON auto activated.";
			MSGS = true;
		}

		if (!running)
		{

			running = true;
			terminate = false;

			run_thread = std::thread(&PostgreSQL::process, this);

			Debug() << "DBMS: start thread, filter: " << Util::Convert::toString(filter.isOn());
			
			if (filter.isOn())
				Debug() << ", Allowed: " << filter.getAllowed();

			Debug() << ", V " << Util::Convert::toString(VD)
					<< ", VP " << Util::Convert::toString(VP)
					<< ", MSGS " << Util::Convert::toString(MSGS)
					<< ", VS " << Util::Convert::toString(VS)
					<< ", BS " << Util::Convert::toString(BS)
					<< ", SAR " << Util::Convert::toString(SAR)
					<< ", ATON " << Util::Convert::toString(ATON)
					<< ", NMEA " << Util::Convert::toString(NMEA);
		}
#else
		throw std::runtime_error("DBMS: no support for PostgeSQL build in.");
#endif
	}

#ifdef HASPSQL

	// Key sets for each table
	static const int keys_vessel_pos[] = {AIS::KEY_LAT, AIS::KEY_LON, AIS::KEY_MMSI, AIS::KEY_STATUS, AIS::KEY_TURN, AIS::KEY_HEADING, AIS::KEY_COURSE, AIS::KEY_SPEED};
	static const int keys_vessel_static[] = {AIS::KEY_MMSI, AIS::KEY_IMO, AIS::KEY_SHIPNAME, AIS::KEY_CALLSIGN, AIS::KEY_TO_BOW, AIS::KEY_TO_STERN, AIS::KEY_TO_STARBOARD, AIS::KEY_TO_PORT, AIS::KEY_DRAUGHT, AIS::KEY_SHIPTYPE, AIS::KEY_DESTINATION, AIS::KEY_ETA};
	static const int keys_basestation[] = {AIS::KEY_LAT, AIS::KEY_LON, AIS::KEY_MMSI};
	static const int keys_sar[] = {AIS::KEY_LAT, AIS::KEY_LON, AIS::KEY_ALT, AIS::KEY_COURSE, AIS::KEY_MMSI, AIS::KEY_SPEED};
	static const int keys_aton[] = {AIS::KEY_LAT, AIS::KEY_LON, AIS::KEY_NAME, AIS::KEY_TO_BOW, AIS::KEY_TO_STERN, AIS::KEY_TO_STARBOARD, AIS::KEY_TO_PORT, AIS::KEY_AID_TYPE, AIS::KEY_MMSI};
	static const int keys_vessel[] = {AIS::KEY_MMSI, AIS::KEY_IMO, AIS::KEY_SHIPNAME, AIS::KEY_CALLSIGN, AIS::KEY_TO_BOW, AIS::KEY_TO_STERN, AIS::KEY_TO_STARBOARD, AIS::KEY_TO_PORT, AIS::KEY_DRAUGHT, AIS::KEY_SHIPTYPE, AIS::KEY_DESTINATION, AIS::KEY_ETA, AIS::KEY_LAT, AIS::KEY_LON, AIS::KEY_STATUS, AIS::KEY_TURN, AIS::KEY_PPM, AIS::KEY_SIGNAL_POWER, AIS::KEY_HEADING, AIS::KEY_ALT, AIS::KEY_AID_TYPE, AIS::KEY_COURSE, AIS::KEY_SPEED};

	void PostgreSQL::appendValue(std::string &values, const JSON::Value &v)
	{
		if (v.isString())
			values += "\'" + escape(v.getString()) + "\'";
		else
			v.to_string(values);
	}

	std::string PostgreSQL::buildInsert(const char *table, const JSON::JSON &data, const int *keys, int nkeys,
										const AIS::Message &msg, const std::string &m, const std::string &s)
	{
		std::string cols, values;

		for (const auto &p : data.getMembers())
		{
			int k = p.Key();
			if (k < 0 || k >= AIS::KEY_COUNT)
				continue;

			for (int i = 0; i < nkeys; i++)
			{
				if (k == keys[i])
				{
					cols += AIS::KeyMap[k][JSON_DICT_FULL];
					cols += ',';
					appendValue(values, p.Get());
					values += ',';
					break;
				}
			}
		}

		cols += "msg_id,station_id,received_at";
		values += m + ',' + s + ",\'" + Util::Convert::toTimestampStr(msg.getRxTimeUnix()) + '\'';

		return "\tINSERT INTO " + std::string(table) + " (" + cols + ") VALUES (" + values + ");\n";
	}

	std::string PostgreSQL::addVessel(const JSON::JSON &data, const AIS::Message &msg, const std::string &m, const std::string &s)
	{
		if (!VD)
			return "";

		std::string cols, values, set = "ON CONFLICT (mmsi) DO UPDATE SET ";

		for (const auto &p : data.getMembers())
		{
			int k = p.Key();
			if (k < 0 || k >= AIS::KEY_COUNT)
				continue;

			for (int i = 0; i < (int)(sizeof(keys_vessel) / sizeof(keys_vessel[0])); i++)
			{
				if (k == keys_vessel[i])
				{
					const char *name = AIS::KeyMap[k][JSON_DICT_FULL];
					cols += name;
					cols += ',';
					set += name;
					set += "=EXCLUDED.";
					set += name;
					set += ',';
					appendValue(values, p.Get());
					values += ',';
					break;
				}
			}
		}

		int type = 1 << msg.type();
		int ch = msg.getChannel() - 'A';
		if (ch < 0 || ch > 4)
			ch = 4;
		ch = 1 << ch;

		cols += "msg_id,station_id,received_at,count,msg_types,channels";
		set += "msg_id=EXCLUDED.msg_id,station_id=EXCLUDED.station_id,received_at=EXCLUDED.received_at,count=ais_vessel.count+1,msg_types=" + std::to_string(type) + "|ais_vessel.msg_types,channels=" + std::to_string(ch) + "|ais_vessel.channels";
		values += m + ',' + s + ",\'" + Util::Convert::toTimestampStr(msg.getRxTimeUnix()) + "\'," + "1," + std::to_string(type) + "," + std::to_string(ch);

		return "\tINSERT INTO ais_vessel (" + cols + ") VALUES (" + values + ")" + set + "; \n";
	}

	void PostgreSQL::Receive(const JSON::JSON *data, int len, TAG &tag)
	{
		const std::lock_guard<std::mutex> lock(queue_mutex);

		if (sql.tellp() > 32768 * 24)
		{
			Warning() << "DBMS: writing to database slow or failed, data lost.";
			sql.str("");
		}

		const JSON::JSON &json = data[0];
		const AIS::Message &msg = *(AIS::Message *)json.binary;

		if (!filter.include(msg))
			return;

		std::string m_id = MSGS ? "m_id" : " NULL";
		std::string s_id = std::to_string(station_id ? station_id : msg.getStation());

		if (MSGS)
		{
			sql << "\tINSERT INTO ais_message (mmsi, station_id, type, received_at,channel, signal_level, ppm) "
				<< "VALUES (" << msg.mmsi() << ',' << s_id << ',' << msg.type() << ",\'" << Util::Convert::toTimestampStr(msg.getRxTimeUnix()) << "\',\'"
				<< (char)msg.getChannel() << "\'," << tag.level << ',' << tag.ppm
				<< ") RETURNING id INTO m_id;\n";
		}

		if (NMEA)
		{
			for (const auto &s : msg.NMEA)
				sql << "\tINSERT INTO ais_nmea (msg_id,station_id,mmsi,received_at,nmea) VALUES (" << m_id << ',' << s_id << ',' << msg.mmsi() << ",\'" << Util::Convert::toTimestampStr(msg.getRxTimeUnix()) << "\',\'" << s << "\');\n";
		}

		switch (msg.type())
		{
		case 1: case 2: case 3: case 18: case 27:
			if (VP) sql << buildInsert("ais_vessel_pos", json, keys_vessel_pos, sizeof(keys_vessel_pos) / sizeof(int), msg, m_id, s_id);
			break;
		case 4:
			if (BS) sql << buildInsert("ais_basestation", json, keys_basestation, sizeof(keys_basestation) / sizeof(int), msg, m_id, s_id);
			break;
		case 5: case 24:
			if (VS) sql << buildInsert("ais_vessel_static", json, keys_vessel_static, sizeof(keys_vessel_static) / sizeof(int), msg, m_id, s_id);
			break;
		case 9:
			if (SAR) sql << buildInsert("ais_sar_position", json, keys_sar, sizeof(keys_sar) / sizeof(int), msg, m_id, s_id);
			break;
		case 19:
			if (VP) sql << buildInsert("ais_vessel_pos", json, keys_vessel_pos, sizeof(keys_vessel_pos) / sizeof(int), msg, m_id, s_id);
			if (VS) sql << buildInsert("ais_vessel_static", json, keys_vessel_static, sizeof(keys_vessel_static) / sizeof(int), msg, m_id, s_id);
			break;
		case 21:
			if (ATON) sql << buildInsert("ais_aton", json, keys_aton, sizeof(keys_aton) / sizeof(int), msg, m_id, s_id);
			break;
		default:
			break;
		}

		sql << addVessel(json, msg, m_id, s_id);

		for (const auto &p : json.getMembers())
		{
			if (p.Key() >= 0 && p.Key() < (int)db_keys.size() && db_keys[p.Key()] != -1)
			{
				std::string temp;
				appendValue(temp, p.Get());
				if (temp.size() > 20)
					temp.resize(20);
				sql << "\tINSERT INTO ais_property (msg_id, key, value) VALUES (" << m_id << ",\'" << db_keys[p.Key()] << "\',\'" << temp << "\');\n";
			}
		}
		sql << "\n";
	}
#endif

	Setting &PostgreSQL::SetKey(AIS::Keys key, const std::string &arg)
	{
		switch (key)
		{
		case AIS::KEY_SETTING_CONN_STR:
			conn_string = arg;
			break;
		case AIS::KEY_SETTING_GROUPS_IN:
			StreamIn<JSON::JSON>::setGroupsIn(Util::Parse::Integer(arg));
			break;
		case AIS::KEY_SETTING_STATION_ID:
			station_id = Util::Parse::Integer(arg);
			break;
		case AIS::KEY_SETTING_INTERVAL:
			INTERVAL = Util::Parse::Integer(arg, 5, 1800);
			break;
		case AIS::KEY_SETTING_MAX_FAILS:
			MAX_FAILS = Util::Parse::Integer(arg);
			break;
		case AIS::KEY_SETTING_NMEA:
			NMEA = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_MSGS:
			MSGS = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_VP:
			VP = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_V:
			VD = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_VS:
			VS = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_BS:
			BS = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_ATON:
			ATON = Util::Parse::Switch(arg);
			break;
		case AIS::KEY_SETTING_SAR:
			SAR = Util::Parse::Switch(arg);
			break;
		default:
			if (!setOptionKey(key, arg) && !filter.SetOptionKey(key, arg))
				throw std::runtime_error("DBMS: unknown option.");
			break;
		}
		return *this;
	}
}
