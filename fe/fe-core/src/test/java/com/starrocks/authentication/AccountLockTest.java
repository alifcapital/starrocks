// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authentication;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.AccountLockInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SetExecutor;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AlterUserAccountLockStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class AccountLockTest {
    static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static AuthenticationMgr authMgr() {
        return ctx.getGlobalStateMgr().getAuthenticationMgr();
    }

    private static void createUser(String sql) throws Exception {
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authMgr().createUser(stmt);
    }

    @Test
    public void testParseAccountLock() throws Exception {
        AlterUserAccountLockStmt lock = (AlterUserAccountLockStmt)
                UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("ALTER USER 'u1'@'%' ACCOUNT LOCK", ctx);
        Assertions.assertTrue(lock.isLock());
        Assertions.assertFalse(lock.isIfExists());

        AlterUserAccountLockStmt unlock = (AlterUserAccountLockStmt)
                UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                        "ALTER USER IF EXISTS 'u1'@'%' ACCOUNT UNLOCK", ctx);
        Assertions.assertFalse(unlock.isLock());
        Assertions.assertTrue(unlock.isIfExists());
    }

    @Test
    public void testAccountLockSqlRoundTrip() throws Exception {
        for (String sql : new String[] {
                "ALTER USER 'u1'@'%' ACCOUNT LOCK",
                "ALTER USER 'u1'@'%' ACCOUNT UNLOCK",
                "ALTER USER IF EXISTS 'u1'@'%' ACCOUNT LOCK",
                "ALTER USER IF EXISTS 'u1'@['example.com'] ACCOUNT UNLOCK"}) {
            AlterUserAccountLockStmt stmt = (AlterUserAccountLockStmt)
                    UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ctx);
            String formatted = AstToSQLBuilder.toSQL(stmt);
            Assertions.assertEquals(sql, formatted);
            AlterUserAccountLockStmt reparsed = (AlterUserAccountLockStmt)
                    UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(formatted, ctx);
            Assertions.assertEquals(stmt.isIfExists(), reparsed.isIfExists());
            Assertions.assertEquals(stmt.isLock(), reparsed.isLock());
            Assertions.assertEquals(stmt.getUser(), reparsed.getUser());
        }
    }

    @Test
    public void testLockGateAndUnlockRestore() throws Exception {
        createUser("create user gateuser identified by 'abc'");
        UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp("gateuser", "%");

        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");
        ctx.setAuthDataSalt(seed);

        // before lock: correct password authenticates
        Assertions.assertEquals(uid,
                AuthenticationHandler.authenticate(ctx, "gateuser", "10.1.1.1", scramble));

        // locked: rejected before password check, regardless of correct password
        authMgr().setAccountLock(uid, true);
        try {
            AuthenticationHandler.authenticate(ctx, "gateuser", "10.1.1.1", scramble);
            Assertions.fail("locked account must not authenticate");
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().toLowerCase().contains("locked"), e.getMessage());
        }

        // unlocked: same password works again (proves password was preserved)
        authMgr().setAccountLock(uid, false);
        Assertions.assertEquals(uid,
                AuthenticationHandler.authenticate(ctx, "gateuser", "10.1.1.1", scramble));
    }

    @Test
    public void testReplaySetAccountLock() throws Exception {
        createUser("create user repluser");
        UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp("repluser", "%");

        // a follower restored from a pre-lock image
        UtFrameUtils.PseudoImage preLock = new UtFrameUtils.PseudoImage();
        authMgr().saveV2(preLock.getImageWriter());
        AuthenticationMgr follower = new AuthenticationMgr();
        follower.loadV2(preLock.getMetaBlockReader());
        Assertions.assertFalse(follower.getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());

        // replaying the journal op (as a follower would) flips the flag, both ways
        AccountLockInfo lockInfo = new AccountLockInfo(uid, true);
        follower.replaySetAccountLock(lockInfo.getUserIdentity(), lockInfo.isLocked());
        Assertions.assertTrue(follower.getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());

        follower.replaySetAccountLock(uid, false);
        Assertions.assertFalse(follower.getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());

        // replaying for an absent user is a safe no-op
        follower.replaySetAccountLock(UserIdentity.createAnalyzedUserIdentWithIp("ghost", "%"), true);
    }

    @Test
    public void testImagePersistsLock() throws Exception {
        createUser("create user imguser");
        UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp("imguser", "%");
        authMgr().setAccountLock(uid, true);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        authMgr().saveV2(image.getImageWriter());

        AuthenticationMgr loaded = new AuthenticationMgr();
        loaded.loadV2(image.getMetaBlockReader());
        Assertions.assertTrue(loaded.getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());
    }

    @Test
    public void testRootCannotBeLocked() throws Exception {
        Assertions.assertThrows(Exception.class, () ->
                UtFrameUtils.parseStmtWithNewParser("ALTER USER root ACCOUNT LOCK", ctx));
    }

    @Test
    public void testNonPrivilegedCallerRejected() throws Exception {
        createUser("create user locktarget");
        createUser("create user nogrant");
        ConnectContext userCtx = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("nogrant", "%"));

        AlterUserAccountLockStmt stmt = (AlterUserAccountLockStmt)
                UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                        "ALTER USER 'locktarget'@'%' ACCOUNT LOCK", ctx);
        // a caller without GRANT on SYSTEM is denied
        Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(stmt, userCtx));
    }

    @Test
    public void testLockSurvivesPasswordChange() throws Exception {
        createUser("create user pwduser identified by 'abc'");
        UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp("pwduser", "%");
        authMgr().setAccountLock(uid, true);

        // a routine password change rebuilds UserAuthenticationInfo; it must NOT clear the lock
        AlterUserStmt alter = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER USER 'pwduser'@'%' IDENTIFIED BY 'xyz'", ctx);
        UserRef u = alter.getUser();
        UserIdentity altUid = new UserIdentity(u.getUser(), u.getHost(), u.isDomain());
        authMgr().alterUser(altUid, new UserAuthenticationInfo(u, alter.getAuthOption()), alter.getProperties());

        Assertions.assertTrue(authMgr().getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());
    }

    @Test
    public void testLockSurvivesSetPassword() throws Exception {
        createUser("create user setpwduser identified by 'abc'");
        UserIdentity uid = UserIdentity.createAnalyzedUserIdentWithIp("setpwduser", "%");
        authMgr().setAccountLock(uid, true);

        // SET PASSWORD funnels through the same alterUser path; the lock must survive
        SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                "SET PASSWORD FOR 'setpwduser'@'%' = PASSWORD('xyz')", ctx);
        new SetExecutor(ctx, setStmt).execute();

        Assertions.assertTrue(authMgr().getUserAuthenticationInfoByUserIdentity(uid).isAccountLocked());
    }

    @Test
    public void testIfExistsMissingUserNoop() throws Exception {
        // full path: parse -> analyze (IF EXISTS lets the missing user through) -> execute -> no-op, no throw
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(
                "ALTER USER IF EXISTS 'ghostlock'@'%' ACCOUNT LOCK", ctx);
        Assertions.assertDoesNotThrow(() -> DDLStmtExecutor.execute(stmt, ctx));
    }
}
