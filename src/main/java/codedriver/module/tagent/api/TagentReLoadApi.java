package codedriver.module.tagent.api;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.dao.mapper.runner.RunnerMapper;
import codedriver.framework.dto.runner.RunnerVo;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.tagent.auth.label.TAGENT_BASE;
import codedriver.framework.tagent.dao.mapper.TagentMapper;
import codedriver.framework.tagent.dto.TagentMessageVo;
import codedriver.framework.tagent.dto.TagentVo;
import codedriver.framework.tagent.enums.TagentAction;
import codedriver.framework.tagent.exception.TagentActionNotFoundException;
import codedriver.framework.tagent.exception.TagentIdNotFoundException;
import codedriver.framework.tagent.tagenthandler.core.ITagentHandler;
import codedriver.framework.tagent.tagenthandler.core.TagentHandlerFactory;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@AuthAction(action = TAGENT_BASE.class)
@OperationType(type = OperationTypeEnum.OPERATE)
public class TagentReLoadApi extends PrivateApiComponentBase {

    @Resource
    TagentMapper tagentMapper;

    @Resource
    RunnerMapper runnerMapper;

    @Override
    public String getName() {
        return "重启Tagent";
    }

    @Override
    public String getToken() {
        return "tagent/exec/reload";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "tagentId", type = ApiParamType.LONG, isRequired = true, desc = "tagent id")
    })
    @Description(desc = "重启Tagent接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        TagentMessageVo message = JSONObject.toJavaObject(paramObj, TagentMessageVo.class);
        JSONObject result = null;
        TagentVo tagent = tagentMapper.getTagentById(message.getTagentId());
        if (tagent == null) {
            throw new TagentIdNotFoundException(message.getTagentId());
        }
        RunnerVo runner = runnerMapper.getRunnerById(tagent.getRunnerId());
        ITagentHandler tagentHandler = TagentHandlerFactory.getInstance(TagentAction.RELOAD.getValue());
        if (tagentHandler == null) {
            throw new TagentActionNotFoundException(TagentAction.RELOAD.getValue());
        } else {
            result = tagentHandler.execTagentCmd(message, tagent, runner);
        }
        return result;
    }


}