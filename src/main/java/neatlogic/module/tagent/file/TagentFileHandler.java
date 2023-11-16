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
package neatlogic.module.tagent.file;

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthActionChecker;
import neatlogic.framework.file.core.FileTypeHandlerBase;
import neatlogic.framework.file.dto.FileVo;
import neatlogic.framework.tagent.auth.label.TAGENT_BASE;
import org.springframework.stereotype.Component;

@Component
public class TagentFileHandler extends FileTypeHandlerBase {


    @Override
    public boolean valid(String userUuid, FileVo fileVo, JSONObject jsonObj) {
        return true;
    }

    @Override
    public boolean validDeleteFile(FileVo fileVo) {
        return AuthActionChecker.check(TAGENT_BASE.class.getSimpleName());
    }

    @Override
    public String getDisplayName() {
        return "tagent附件";
    }


    @Override
    public String getName() {
        return "TAGENT";
    }

    @Override
    protected boolean myDeleteFile(FileVo fileVo, JSONObject paramObj) {
        return true;
    }
}
