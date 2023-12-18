package com.example.esstudy;

import com.example.esstudy.model.VideoDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


@SpringBootTest
class XdclassEsProjectApplicationTests {

    @Autowired
    private ElasticsearchTemplate restTemplate;


    @Test
    void existsIndex() {

        IndexOperations indexOperations = restTemplate.indexOps(VideoDTO.class);
        boolean exists = indexOperations.exists();
        System.out.println(exists);

    }


    @Test
    void createIndex() {
        IndexOperations indexOperations = restTemplate.indexOps(VideoDTO.class);

        if (indexOperations.exists()) {
            indexOperations.delete();
        }

        //创建索引库
        indexOperations.create();

        indexOperations.putMapping();

    }


    @Test
    void insert() {
        VideoDTO videoDTO = new VideoDTO();
        videoDTO.setId(1L);
        videoDTO.setTitle("小滴课堂架构大课和Spring Cloud");
        videoDTO.setCreateTime(LocalDateTime.now());
        videoDTO.setDuration(100);
        videoDTO.setCategory("后端");
        videoDTO.setDescription("这个是综合大型课程，包括了jvm，redis，新版spring boot3.x，架构，监控，性能优化，算法，高并发等多方面内容");
        VideoDTO saved = restTemplate.save(videoDTO);
        System.out.println(saved);
    }


    @Test
    void update() {
        VideoDTO videoDTO = new VideoDTO();
        videoDTO.setId(1L);
        videoDTO.setTitle("小滴课堂架构大课和Spring Cloud V2");
        videoDTO.setCreateTime(LocalDateTime.now());
        videoDTO.setDuration(102);
        videoDTO.setCategory("后端");
        videoDTO.setDescription("这个是综合大型课程，包括了jvm，redis，新版spring boot3.x，架构，监控，性能优化，算法，高并发等多方面内容");

        VideoDTO saved = restTemplate.save(videoDTO);
        System.out.println(saved);
    }


    @Test
    void batchInsert() {
        List<VideoDTO> list = new ArrayList<>();
        list.add(new VideoDTO(2L, "老王录制的按摩课程", "主要按摩和会所推荐", 123, "后端"));
        list.add(new VideoDTO(3L, "冰冰的前端性能优化", "前端高手系列", 100042, "前端"));
        list.add(new VideoDTO(4L, "海量数据项目大课", "D哥的后端+大数据综合课程", 5432345, "后端"));
        list.add(new VideoDTO(5L, "小滴课堂永久会员", "可以看海量专题课程，IT技术持续充电平台", 6542, "后端"));
        list.add(new VideoDTO(6L, "大钊-前端低代码平台", "高效开发底层基础平台，效能平台案例", 53422, "前端"));
        list.add(new VideoDTO(7L, "自动化测试平台大课", "微服务架构下的spring cloud架构大课，包括jvm,效能平台", 6542, "后端"));


        Iterable<VideoDTO> result = restTemplate.save(list);
        System.out.println(result);
    }


    @Test
    void searchById() {
        VideoDTO videoDTO = restTemplate.get("3", VideoDTO.class);
        System.out.println(videoDTO);
    }


    @Test
    void deleteById() {
        String delete = restTemplate.delete("2", VideoDTO.class);
        System.out.println(delete);
    }


    @Test
    void searchAll() {

        SearchHits<VideoDTO> search = restTemplate.search(Query.findAll(), VideoDTO.class);

        List<SearchHit<VideoDTO>> searchHits = search.getSearchHits();

        List<VideoDTO> list = new ArrayList<>();

        searchHits.forEach(hit -> {
            list.add(hit.getContent());
        });
        System.out.println(list);
    }


    @Test
    void matchQuery() {

        Query query = NativeQuery.builder()
                .withQuery(q -> q.match(m -> m.field("description").query("spring"))).build();

        SearchHits<VideoDTO> search = restTemplate.search(query, VideoDTO.class);

        List<SearchHit<VideoDTO>> searchHits = search.getSearchHits();

        List<VideoDTO> list = new ArrayList<>();

        searchHits.forEach(hit -> {
            list.add(hit.getContent());
        });
        System.out.println(list);
    }


    @Test
    void searchPage() {
        Query query = NativeQuery.builder()
                .withQuery(Query.findAll())
                .withPageable(Pageable.ofSize(3).withPage(1))
                .build();

        SearchHits<VideoDTO> search = restTemplate.search(query, VideoDTO.class);

        List<SearchHit<VideoDTO>> searchHits = search.getSearchHits();

        List<VideoDTO> list = new ArrayList<>();

        searchHits.forEach(hit -> {
            list.add(hit.getContent());
        });
        System.out.println(list);

    }

    @Test
    void searchSort() {
        Query query = NativeQuery.builder()
                .withQuery(Query.findAll())
                .withPageable(Pageable.ofSize(10).withPage(0))
                .withSort(Sort.by("duration").ascending())
                .build();

        SearchHits<VideoDTO> search = restTemplate.search(query, VideoDTO.class);

        List<SearchHit<VideoDTO>> searchHits = search.getSearchHits();

        List<VideoDTO> list = new ArrayList<>();

        searchHits.forEach(hit -> {
            list.add(hit.getContent());
        });
        System.out.println(list);

    }
}
