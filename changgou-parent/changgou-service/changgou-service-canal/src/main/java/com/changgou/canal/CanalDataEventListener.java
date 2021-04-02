package com.changgou.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xpand.starter.canal.annotation.*;

/**
 * 实现数据库监听
 */
@CanalEventListener
public class CanalDataEventListener {

    /**
     *
     *      * 增加监听
     * @param eventType 当前操作类型，增加数据
     * @param rowData 发送变更的一行数据
     */
    @InsertListenPoint
    public void onEventInsert(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
    }

    /**
     * 修改监听
     * @param eventType 当前操作类型，增加数据
     *      * @param rowData 发送变更的一行数据
     */

    @UpdateListenPoint
    public void onEventUpadate(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("修改前：列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("修改后：列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
    }

    /**
     * 删除监听
     *      * @param eventType 当前操作类型，增加数据
     *      *      * @param rowData 发送变更的一行数据
     * @param eventType
     * @param rowData
     */
    @DeleteListenPoint
    public void onEventDelete(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
    }

    /**
     * 自定义监听
     *      * @param eventType 当前操作类型，增加数据
     *      *      * @param rowData 发送变更的一行数据
     * @param eventType
     * @param rowData
     */
    @ListenPoint(
            eventType = {CanalEntry.EventType.DELETE, CanalEntry.EventType.UPDATE}, //监听类型
            schema = {"changgou_content"},  //指定监听的数据
            table = {"tb_content"}, //指定监听的表
            destination = "example" //指定实例地址
    )
    public void onEventCustomeUpdate(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            System.out.println("自定义操作前，列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            System.out.println("自定义操作前，列名：" + column.getName() + "---变更的数据：" + column.getValue());
        }
    }
}
