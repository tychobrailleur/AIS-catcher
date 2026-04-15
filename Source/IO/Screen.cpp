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

#include "Screen.h"

namespace IO
{
	void ScreenOutput::Receive(const AIS::GPS *data, int len, TAG &tag)
	{
		if (fmt == MessageFormat::SILENT)
			return;

		thread_local std::string buf;
		for (int i = 0; i < len; i++)
		{
			if (!filter.includeGPS())
				continue;
			buf.clear();
			switch (fmt)
			{
			case MessageFormat::NMEA:
			case MessageFormat::NMEA_TAG:
			case MessageFormat::FULL:
				buf += data[i].getNMEA();
				break;
			default:
				buf += data[i].getJSON();
				break;
			}
			buf += '\n';
			std::cout.write(buf.data(), buf.size());
		}
	}

	void ScreenOutput::Receive(const AIS::Message *data, int len, TAG &tag)
	{
		if (fmt == MessageFormat::SILENT)
			return;

		thread_local std::string buf;
		for (int i = 0; i < len; i++)
		{
			if (!filter.include(data[i]))
				continue;
			buf.clear();
			switch (fmt)
			{
			case MessageFormat::NMEA:
				for (const auto &s : data[i].sentences())
				{
					buf += s;
					buf += '\n';
				}
				break;
			case MessageFormat::NMEA_TAG:
				data[i].getNMEATagBlock(buf);
				break;
			case MessageFormat::FULL:
				for (const auto &s : data[i].sentences())
				{
					buf += s;
					buf += " ( ";
					if (data[i].getLength() > 0)
					{
						buf += "MSG: ";
						buf += std::to_string(data[i].type());
						buf += ", REPEAT: ";
						buf += std::to_string(data[i].repeat());
						buf += ", MMSI: ";
						buf += std::to_string(data[i].mmsi());
					}
					else
					{
						buf += "empty";
					}
					if ((tag.mode & 1) && tag.ppm != PPM_UNDEFINED && tag.level != LEVEL_UNDEFINED)
					{
						char tmp[64];
						std::snprintf(tmp, sizeof(tmp), ", signalpower: %g, ppm: %g", tag.level, tag.ppm);
						buf += tmp;
					}
					if (tag.mode & 2)
					{
						buf += ", timestamp: ";
						buf += data[i].getRxTime();
					}
					if (data[i].getStation())
					{
						buf += ", ID: ";
						buf += std::to_string(data[i].getStation());
					}
					buf += ")\n";
				}
				break;
			case MessageFormat::JSON_NMEA:
				data[i].getNMEAJSON(buf, tag, include_sample_start, "", "\n");
				break;
			default:
				continue;
			}
			std::cout.write(buf.data(), buf.size());
		}
	}

	void ScreenOutput::Receive(const JSON::JSON *data, int len, TAG &tag)
	{
		thread_local std::string buf;
		for (int i = 0; i < len; i++)
		{
			if (!filter.include(*(AIS::Message *)data[i].binary))
				continue;
			buf.clear();
			builder.stringify(data[i], buf, "\n");
			std::cout.write(buf.data(), buf.size());
		}
	}
}
