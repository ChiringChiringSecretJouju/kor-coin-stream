두번째 임무는 core/dto 파일 안에는 io 와 internal 이 있는데 

이 internal 안에는 dataclass(내부에서 옴직이는) 애들이 존재해 그중에 
@dataclass(slots=True, frozen=True, eq=True, repr=False, match_args=False, kw_only=True)  이런식으로 있는데 너무 길어서 
Internal .py를 조사해서 @dataclass 데코레이터를 일원화 해서 사용하기 좋게 만드는것이 두번쨰 임무 

이거 써줘 