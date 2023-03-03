/*
 * Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.
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

package neatlogic.module.tagent.tagenthandler.handler;

import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.cmdb.crossover.IResourceAccountCrossoverMapper;
import neatlogic.framework.cmdb.dto.resourcecenter.AccountVo;
import neatlogic.framework.cmdb.exception.resourcecenter.ResourceCenterAccountNotFoundException;
import neatlogic.framework.crossover.CrossoverServiceFactory;
import neatlogic.framework.dto.RestVo;
import neatlogic.framework.dto.runner.RunnerVo;
import neatlogic.framework.integration.authentication.enums.AuthenticateType;
import neatlogic.framework.tagent.dto.TagentMessageVo;
import neatlogic.framework.tagent.dto.TagentVo;
import neatlogic.framework.tagent.enums.TagentAction;
import neatlogic.framework.tagent.exception.TagentActionFailedException;
import neatlogic.framework.tagent.exception.TagentRunnerConnectRefusedException;
import neatlogic.framework.tagent.tagenthandler.core.TagentHandlerBase;
import neatlogic.framework.util.RestUtil;
import neatlogic.module.tagent.api.TagentLogDownLoadApi;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URLEncoder;

@Component
public class TagentLogDownloadHandler extends TagentHandlerBase {
    private final static Logger logger = LoggerFactory.getLogger(TagentLogDownLoadApi.class);

    @Override
    public String getHandler() {
        return null;
    }

    @Override
    public String getHandlerName() {
        return null;
    }

    @Override
    public String getName() {
        return TagentAction.DOWNLOAD_LOG.getValue();
    }

    @Override
    public JSONObject myExecTagentCmd(TagentMessageVo message, TagentVo tagentVo, RunnerVo runnerVo) throws Exception {
        IResourceAccountCrossoverMapper resourceAccountCrossoverMapper = CrossoverServiceFactory.getApi(IResourceAccountCrossoverMapper.class);
        //验证tagent对应的帐号是否存在，以便后续从该帐号获取对应密文
        AccountVo accountVo = resourceAccountCrossoverMapper.getAccountById(tagentVo.getAccountId());
        if (accountVo == null) {
            throw new ResourceCenterAccountNotFoundException();
        }
        JSONObject params = JSONObject.parseObject(JSONObject.toJSONString(message));
        params.put("type", message.getName());
        params.put("ip", tagentVo.getIp());
        params.put("port", (tagentVo.getPort()).toString());
        params.put("credential", accountVo.getPasswordCipher());
        String result = null;
        ServletOutputStream outputStream = null;
        InputStream inputStream = null;
        String url = runnerVo.getUrl() + "/api/binary/tagent/log/download";
        try {
            RestVo restVo = new RestVo.Builder(url, AuthenticateType.BUILDIN.getValue()).setPayload(params).build();
            result = RestUtil.sendPostRequest(restVo);
            JSONObject resultJson = JSONObject.parseObject(result);
            if (!resultJson.containsKey("Data")) {
                throw new TagentActionFailedException(runnerVo, resultJson.getString("Message"));
            }
            String dataStr = resultJson.getString("Data");
            inputStream = new ByteArrayInputStream(dataStr.getBytes());
            HttpServletResponse response = UserContext.get().getResponse();
            response.setHeader("Content-Disposition", " attachment; filename=\"" + URLEncoder.encode(message.getPath(), "UTF-8") + "\"");
            response.setContentType("application/octet-stream");
            outputStream = response.getOutputStream();
            IOUtils.copyLarge(inputStream, outputStream);
            outputStream.flush();
        } catch (JSONException ex) {
            throw new TagentRunnerConnectRefusedException(url, result);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return null;
    }
}
