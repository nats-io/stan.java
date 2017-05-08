/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationMode;
import org.slf4j.LoggerFactory;

class LogVerifier {
    @Mock
    private Appender<ILoggingEvent> mockAppender;
    // Captor is genericized with ch.qos.logback.classic.spi.LoggingEvent
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;

    void setup() {
        MockitoAnnotations.initMocks(this);
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(mockAppender);
    }

    void teardown() {
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.detachAppender(mockAppender);
    }

    LogVerifier() {

    }

    void verifyLogMsgEquals(Level level, String msg) {
        verifyLogMsg(level, msg, true, times(1));
    }

    void verifyLogMsgEquals(Level level, String msg, VerificationMode mode) {
        verifyLogMsg(level, msg, true, mode);
    }


    void verifyLogMsgMatches(Level level, String msg) {
        verifyLogMsg(level, msg, false, times(1));
    }

    private void verifyLogMsg(Level level, String pattern, boolean exact, VerificationMode mode) {
        // Now verify our logging interactions
        verify(mockAppender, mode).doAppend(captorLoggingEvent.capture());
        List<LoggingEvent> events = captorLoggingEvent.getAllValues();
        List<LoggingEvent> matches = new ArrayList<>();
        for (LoggingEvent ev : events) {
            boolean matched = ev.getLevel().equals(level);
            if (matched && exact) {
                matched = ev.getFormattedMessage().equals(pattern);
            } else if (matched) {
                matched = Pattern.matches(pattern, ev.getFormattedMessage());
            }

            if (matched) {
                matches.add(ev);
            }
        }
        boolean success = false;
        String err = null;
        switch (matches.size()) {
            case 0:
                err = String.format("No matching event(s) found for Level=%s, Message='%s'",
                        level, pattern);
                break;
            case 1:
                success = true;
                break;
            default:
                err = "Too many messages";
                success = (mode == atLeastOnce());
                break;
        }
        if (!success) {
            if (events.size() > 0) {
                System.err.printf("%s\nFound these events:\n\n", err);
                printLoggingEvents(events);
            }
            fail(err);
        }
    }

    private void printLoggingEvents(List<LoggingEvent> events) {
        List<LoggingEvent> matches = new ArrayList<>();
        for (LoggingEvent ev : events) {
            System.err.printf("[%s] %s\n", ev.getLevel(), ev.getFormattedMessage());
        }
    }


}
