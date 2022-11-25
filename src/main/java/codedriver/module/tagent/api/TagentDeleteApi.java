/*
 * Copyright(c) 2021 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.tagent.api;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.cmdb.crossover.IResourceAccountCrossoverMapper;
import codedriver.framework.cmdb.crossover.IResourceCenterAccountCrossoverService;
import codedriver.framework.cmdb.dto.resourcecenter.AccountVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.crossover.CrossoverServiceFactory;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.tagent.auth.label.TAGENT_BASE;
import codedriver.framework.tagent.dao.mapper.TagentMapper;
import codedriver.framework.tagent.dto.TagentVo;
import codedriver.framework.tagent.enums.TagentStatus;
import codedriver.framework.tagent.exception.TagentHasBeenConnectedException;
import codedriver.framework.tagent.exception.TagentIdNotFoundException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
@Transactional
@AuthAction(action = TAGENT_BASE.class)
@OperationType(type = OperationTypeEnum.DELETE)
public class TagentDeleteApi extends PrivateApiComponentBase {

    @Resource
    TagentMapper tagentMapper;

    @Override
    public String getName() {
        return "删除tagent";
    }

    @Override
    public String getToken() {
        return "tagent/delete";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", isRequired = true, type = ApiParamType.LONG, desc = "tagent id")
    })
    @Output({
    })
    @Description(desc = "删除tagent接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Long id = paramObj.getLong("id");
        if (id != null) {
            TagentVo tagent = tagentMapper.getTagentById(id);
            if (tagent == null) {
                throw new TagentIdNotFoundException(id);
            }
            if (!StringUtils.equals(tagent.getStatus(), TagentStatus.CONNECTED.getValue())) {
                List<Long> deletedAccountIdList = new ArrayList<>();
                deletedAccountIdList.add(tagent.getAccountId());
                List<String> deletedIpList = tagentMapper.getTagentIpListByTagentIpAndPort(tagent.getIp(), tagent.getPort());

                //删除tagent
                tagentMapper.deleteTagentById(id);
                tagentMapper.deleteAllIpByTagentId(id);

                IResourceAccountCrossoverMapper resourceAccountCrossoverMapper = CrossoverServiceFactory.getApi(IResourceAccountCrossoverMapper.class);
                if (CollectionUtils.isNotEmpty(deletedIpList)) {
                    //两个tagent之间的包含ip列表存在有相同部分的情况，所以根据需要删除的包含ip列表获取仍然需要的tagent的ip
                    List<String> neededIpList = tagentMapper.getTagentIpListByIpList(deletedIpList);
                    if (CollectionUtils.isNotEmpty(neededIpList)) {
                        deletedIpList = deletedIpList.stream().filter(item -> !neededIpList.contains(item)).collect(toList());
                    }
                    for (String ip : deletedIpList) {
                        AccountVo deletedAccountVo = resourceAccountCrossoverMapper.getResourceAccountByIpAndPort(ip, tagent.getPort());
                        if (deletedAccountVo != null) {
                            deletedAccountIdList.add(deletedAccountVo.getId());
                        }
                    }
                }

                //删掉该tagent account
                IResourceCenterAccountCrossoverService accountService = CrossoverServiceFactory.getApi(IResourceCenterAccountCrossoverService.class);
                accountService.deleteAccount(deletedAccountIdList);
            } else {
                throw new TagentHasBeenConnectedException(tagent);
            }
        }
        return null;
    }
}
