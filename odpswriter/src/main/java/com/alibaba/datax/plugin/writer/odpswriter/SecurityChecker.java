/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SecurityChecker {
    private static Logger LOG = LoggerFactory.getLogger(SecurityChecker.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    public static Configuration flushAccessKeyID(Configuration param) {
        Map<String, String> envProp = System.getenv();

        return doFlushAccessKeyID(param, envProp);
    }

    private static Configuration doFlushAccessKeyID(Configuration param,
                                                    Map<String, String> envProp) {

        String accessId = null;
        String accessKey = null;

        String skynetAccessID = envProp.get(Constant.SKYNET_ACCESSID);
        String skynetAccessKey = envProp.get(Constant.SKYNET_ACCESSKEY);

        if (StringUtils.isNotBlank(skynetAccessID)
                || StringUtils.isNotBlank(skynetAccessKey)) {
            // 环境变量中，如果存在SKYNET_ACCESSID/SKYNET_ACCESSKEy（只要有其中一个变量，则认为一定是两个都存在的！），则使用其值作为odps
            // 的accessid/accesskey(会解密)
            LOG.info("try to get accessid/accesskey from skynet-env.");
            accessId = skynetAccessID;
            accessKey = DESCipher.decrypt(skynetAccessKey);
            if (StringUtils.isNotBlank(accessKey)) {
                param.set(Key.ACCESS_ID, accessId);
                param.set(Key.ACCESS_KEY, accessKey);
                LOG.info(
                        "get accessid/accesskey from skynet-env successfully, and access_id=[{}]",
                        accessId);
            } else {
                String errMsg = String
                        .format("get accessid/accesskey from skynet-env failed, and access_id=[%s]",
                                accessId);
                LOG.error(errMsg);
                throw new DataXException(null, errMsg);
            }
        } else {
            // 采用用户配置的accessid/accesskey，不需要对accesskey 解密。
            LOG.info("try to get accessid/accesskey from user config.");
            accessId = param.getString(Key.ACCESS_ID);
            accessKey = param.getString(Key.ACCESS_KEY);
            LOG.info(
                    "get accessid/accesskey from user config successfully, and access_id=[{}]",
                    accessId);
        }

        if (IS_DEBUG) {
            LOG.debug("access-id:[{}], access-key:[{}] .", accessId, accessKey);
        }

        LOG.info("access-id:[{}] .", accessId);

        return param;
    }
}
