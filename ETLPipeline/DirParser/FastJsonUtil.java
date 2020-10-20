package ETLPipeline.DirParser;

import com.alibaba.fastjson.JSON;

public class FastJsonUtil {

	/**
	 * bean to json
	 * @param obj
	 * @return
	 */
	public static String bean2Json(Object obj) {
		return JSON.toJSONString(obj);
	}
	
	/**
	 * json to bean
	 * @param <T>
	 * @param jsonStr
	 * @param objClass
	 * @return
	 */
	public static <T> T json2Bean(String jsonStr, Class<T> objClass) {
		return JSON.parseObject(jsonStr, objClass);
	}
	
}
