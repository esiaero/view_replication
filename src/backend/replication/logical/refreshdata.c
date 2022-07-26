/*-------------------------------------------------------------------------
 *
 * refreshdata.c
 *	  Logical refresh message.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/refreshdata.c
 *
 * NOTES
 *
 * Logical 
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/refreshdata.h"
#include "utils/memutils.h"

XLogRecPtr
LogLogicalRefreshData(const char *prefix, const char *message, Oid matviewOid,
				      bool concurrent, bool skipData, bool isCompleteQuery)
{
	xl_logical_refresh_data xlrec;
	/*
	 * Force xid to be allocated since we're emitting a transactional message.
	 */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	xlrec.dbId = MyDatabaseId;
	xlrec.matviewId = matviewOid;
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = strlen(message);
	xlrec.flags = 0;
	if (concurrent)
		xlrec.flags |= XLL_REFRESH_CONCURR;
	if (skipData)
		xlrec.flags |= XLL_REFRESH_SKIPDATA;
	if (isCompleteQuery)
		xlrec.flags |= XLL_REFRESH_COMPLETEQUERY;
	
	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalRefreshData);
	XLogRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	XLogRegisterData(unconstify(char *, message), xlrec.message_size);

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	return XLogInsert(RM_LOGICALREFRESHDATA_ID, XLOG_LOGICAL_REFRESH_DATA);
}

/*
 * Redo is basically just noop for logical decoding refresh data.
 */
void
logicalrefreshdata_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_LOGICAL_REFRESH_DATA)
		elog(PANIC, "logicalrefreshdata_redo: unknown op code %u", info);

	/* This is only interesting for logical decoding, see decode.c. */
}
