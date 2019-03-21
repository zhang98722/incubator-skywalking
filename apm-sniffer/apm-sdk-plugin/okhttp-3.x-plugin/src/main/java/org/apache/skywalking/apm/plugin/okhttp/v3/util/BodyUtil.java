/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.plugin.okhttp.v3.util;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.util.StringUtil;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * fetch request body util and repackage back
 *
 * @author: zhanglin
 * @create: 2019/3/19 10:51
 */
public class BodyUtil {

    public static final String              DESCRIPTION_BIG_FILE                        = "#BIG FILE#";
    public static final String              DESCRIPTION_BLANK                           = "#BLANK#";

    public static final int                 MAX_CONTENT_LENGTH                          = 1024 * 2;

    public static String getRequestBody(Request request) throws Throwable {

        Field bodyField = Request.class.getDeclaredField("body");
        bodyField.setAccessible(true);

        Object body = bodyField.get(request);
        if (body == null) {
            return DESCRIPTION_BLANK;
        }

        RequestBody requestBody = (RequestBody)body;
        if (requestBody.contentLength() > MAX_CONTENT_LENGTH) {
            return DESCRIPTION_BIG_FILE;
        }
        Buffer buffer = new Buffer();
        requestBody.writeTo(buffer);
        byte[] byteArray = buffer.readByteArray();
        String result = new String(byteArray, StandardCharsets.UTF_8);

        bodyField.set(request, RequestBody.create(requestBody.contentType(), byteArray));

        return result;
    }

    public static String getResponseBody(Response response) throws  Throwable {
        Field bodyField = Response.class.getDeclaredField("body");
        bodyField.setAccessible(true);

        Object body = bodyField.get(response);
        if (body == null) {
            return DESCRIPTION_BLANK;
        }

        ResponseBody responseBody = (ResponseBody)body;
        if (responseBody.contentLength() > MAX_CONTENT_LENGTH) {
            return DESCRIPTION_BIG_FILE;
        }
        byte[] byteArray = responseBody.bytes();
        String result = new String(byteArray, StandardCharsets.UTF_8);

        bodyField.set(response, ResponseBody.create(responseBody.contentType(), byteArray));

        return result;
    }

    public static void log(AbstractSpan span, Request request) throws Throwable {
        String body = getRequestBody(request);
        if (!StringUtil.isEmpty(body)) {
            Map<String,Object> event = new HashMap<>();
            event.put("requestBody", body);
            span.log(System.currentTimeMillis(), event);
        }
    }

    public static void log(AbstractSpan span, Response response) throws  Throwable {
        String body = getResponseBody(response);
        if (!StringUtil.isEmpty(body)) {
            Map<String,Object> event = new HashMap<>();
            event.put("responseBody", body);
            span.log(System.currentTimeMillis(), event);
        }
    }

}
