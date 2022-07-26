/*-------------------------------------------------------------------------
 *
 * refreshdatadesc.c
 *	  rmgr descriptor routines for replication/logical/....c
 *
 * Portions Copyright (c) 2015-2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/refreshdatadesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/refreshdata.h"

void
logicalrefreshdata_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_LOGICAL_REFRESH_DATA)
	{
		xl_logical_refresh_data *xlrec = (xl_logical_refresh_data *) rec;
		char	   *prefix = xlrec->message;
		char	   *message = xlrec->message + xlrec->prefix_size;
		char	   *sep = "";

		Assert(prefix[xlrec->prefix_size] != '\0');

		if (xlrec->flags & XLL_REFRESH_CONCURR)
			appendStringInfoString(buf, "concurrent");
		if (xlrec->flags & XLL_REFRESH_SKIPDATA)
			appendStringInfoString(buf, "skipData");
		if (xlrec->flags & XLL_REFRESH_COMPLETEQUERY)
			appendStringInfoString(buf, "isCompleteQuery");

		appendStringInfo(buf, "prefix \"%s\"; payload (%zu bytes): ",
						 prefix, xlrec->message_size);/* Write message payload as a series of hex bytes */
		for (int cnt = 0; cnt < xlrec->message_size; cnt++)
		{
			appendStringInfo(buf, "%s%02X", sep, (unsigned char) message[cnt]);
			sep = " ";
		}
	}
}

const char *
logicalrefreshdata_identify(uint8 info)
{
	if ((info & ~XLR_INFO_MASK) == XLOG_LOGICAL_REFRESH_DATA)
		return "REFRESH DATA";

	return NULL;
}
