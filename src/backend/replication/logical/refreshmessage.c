/*-------------------------------------------------------------------------
 *
 * refreshmessage.c
 *	  Logical refresh message.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/refreshmessage.c
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
#include "replication/refreshmessage.h"
#include "utils/memutils.h"

/*
 * Write logical decoding refresh message into XLog.
 */
XLogRecPtr
LogLogicalRefreshMessage(const char *prefix, Oid roleoid, const char *message, Oid matviewOid,
						 bool concurrent, bool skipData, bool isCompleteQuery)
{
	xl_logical_refresh_message xlrec;
	const char *role;

	role =  GetUserNameFromId(roleoid, false);

	/*
	 * Force xid to be allocated since we're emitting a transactional message.
	 */
	Assert(IsTransactionState());
	GetCurrentTransactionId();

	xlrec.dbId = MyDatabaseId;
	xlrec.matviewId = matviewOid;
	/* trailing zero is critical; see logicalrefreshmsg_desc */
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.role_size = strlen(role) + 1;
	xlrec.search_path_size = strlen(namespace_search_path) + 1;
	xlrec.message_size = strlen(message);
	xlrec.flags = 0;
	if (concurrent)
		xlrec.flags |= XLL_REFRESH_CONCURR;
	if (skipData)
		xlrec.flags |= XLL_REFRESH_SKIPDATA;
	if (isCompleteQuery)
		xlrec.flags |= XLL_REFRESH_COMPLETEQUERY;
	
	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalRefreshMessage);
	XLogRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	XLogRegisterData(unconstify(char *, role), xlrec.role_size);
	XLogRegisterData(namespace_search_path, xlrec.search_path_size);
	XLogRegisterData(unconstify(char *, message), xlrec.message_size);

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	return XLogInsert(RM_LOGICALREFRESHMSG_ID, XLOG_LOGICAL_REFRESH_MESSAGE);
}

/*
 * Redo is basically just noop for logical decoding refresh messages.
 */
void
logicalrefreshmsg_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_LOGICAL_REFRESH_MESSAGE)
		elog(PANIC, "logicalrefreshmsg_redo: unknown op code %u", info);

	/* This is only interesting for logical decoding, see decode.c. */
}
