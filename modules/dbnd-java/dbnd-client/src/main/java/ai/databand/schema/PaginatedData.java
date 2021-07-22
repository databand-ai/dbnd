package ai.databand.schema;

import java.util.List;

public class PaginatedData<T> {

    private List<T> data;

    private PaginationMeta meta;

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public PaginationMeta getMeta() {
        return meta;
    }

    public void setMeta(PaginationMeta meta) {
        this.meta = meta;
    }
}
