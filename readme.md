## Spotify

- [X] rds
    - [X] raw query로 rds에 artist 입력
    - [X] artist 기져와서 batch로 artist_genre 삽입
- [X] dynamo-db
    - [X] rds의 artist_id로 top_tracks, albums를 dynamo-db에 저장
- [ ] s3  
    - [ ] dynamo-db의 artist_id로 top_tarcks, audio-features를 parquet으로 s3에 저장
- [ ] athena
    - [ ] s3 저장 데이터 쿼리
- [ ] spark
- [ ] lambda