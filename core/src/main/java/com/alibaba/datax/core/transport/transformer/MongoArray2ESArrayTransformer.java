package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;
import java.util.Map;

/**
 * @Auther: wangfanming
 * @Date: 2019/7/4 15:47
 * @Description:
 */
public class MongoArray2ESArrayTransformer extends Transformer {

    private int columnIndex;

    public MongoArray2ESArrayTransformer(){
        super.setTransformerName("MongoArray2ESArrayTransformer");
    }

    @Override
    public Record evaluate(Record record,  Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("Hiding transformer 缺少参数");
            }
            columnIndex = (Integer) paras[0];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);

        try{
            String img = column.asString();
            int startIndex = img.indexOf("[");
            int endIndex = img.lastIndexOf("]");
            if(startIndex + 1 != endIndex){
                String relStr = img.replace("http", "\"http")
                        .replace(",", "\",\"")
                        .replace("]","\"]");
                record.setColumn(columnIndex,new StringColumn(relStr));

            }else{
                return record;
            }



        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
