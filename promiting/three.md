ZAOC(Zero-Ambiguity, Zero-Cost OOP in Python) 의 철학 중 하나인 DTO를 나열해보면 
    1. 바깥쪽 IO 부분에서는 pydantic 사용
    2. 안쪽 부분은 dataclass 사용 

크게 보면 이렇게 볼 수 있습니다 즉, 
1. 바깥쪽으로 나가고 들어오는것에 대한 검증은 pydantic으로 하고 
2. 안쪽에서 일어나는 것은 dataclass를 사용하는 것입니다

TypeDict는 사용하지 않습니다 


수도 코드로 DTO의 흐름을 보면 다음과 같습니다 

if kafka_consumer가 메시지를 소모했다면 {
    a -> pydantic 검증 
    if a pass {
        b -> dataclass 변환 (이과정에서 adapater가 필요 할 수 도 있음 )
    }
}

if kafka_producer가 메시지를 보내기 전에 {
    a -> dataclass 를 pydantic로 변환 (이과정에서 adapter가 필요할 수 도 있음)
    b -> 데이터(a) 검증
    if b pass {
        c -> kafka producer 전송
    }
}

pydantic -> dataclass 으로 변환하는 과정에서 adapter가 필요할 수 도 있습니다
이와 반대도 똑같다고 할 수 있겠죠 