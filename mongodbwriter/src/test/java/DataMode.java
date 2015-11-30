/**
 * com.alibaba.datax.plugin.writer.mongodbwriter
 *
 * @Date 15/10/10
 * @Time 下午9:06
 * @Author Shuoshuo.wang@corp.elong.com
 */
public class DataMode {

    private String type;

    private Double[] coord;

    private Long[] longs;

    private Integer[] integers;

    private String[] strings;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double[] getCoord() {
        return coord;
    }

    public void setCoord(Double[] coord) {
        this.coord = coord;
    }

    public Long[] getLongs() {
        return longs;
    }

    public void setLongs(Long[] longs) {
        this.longs = longs;
    }

    public Integer[] getIntegers() {
        return integers;
    }

    public void setIntegers(Integer[] integers) {
        this.integers = integers;
    }

    public String[] getStrings() {
        return strings;
    }

    public void setStrings(String[] strings) {
        this.strings = strings;
    }
}
