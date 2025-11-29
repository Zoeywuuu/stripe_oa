"""
问题描述
您需要设计一个风险评分系统，根据一系列交易数据和动态规则来计算商户的最终风险评分。系统处理三份输入数据：交易列表、规则列表和商户初始评分列表。您需要按顺序处理每笔交易，并根据以下规则更新相应商户的风险评分。

输入数据
List<String> transaction_list

描述：一个包含交易记录的字符串列表。
格式：每条字符串由逗号分隔，包含四个部分："商户ID,交易金额,客户ID,交易小时"。
数据类型：String, int, String, int。

示例：
{
  "merchant_1,1400,customer_1,10",
  "merchant_1,1000,customer_1,10",
  "merchant_2,2000,customer_1,15",
  "merchant_2,500,customer_2,22"
}

List<String> rules_list
描述：一个与 transaction_list 一一对应的规则列表。rules_list 中的第 i 条规则适用于 transaction_list 中的第 i 笔交易。
格式：每条字符串由逗号分隔，包含四个部分："最低交易金额,乘法因子,加法数值,罚金"。
数据类型：int, int, int, int。
示例：
{
  "1000,2,9,15",  // 对应第一条交易
  "2000,3,5,9",   // 对应第二条交易
  "500,3,8,19",   // 对应第三条交易
  "1600,2,17,9"   // 对应第四条交易
}

List<String> merchants_list
描述：一个包含商户及其初始风险评分的列表。
格式：每条字符串由逗号分隔，包含两个部分："商户ID,初始风险评分"。
数据类型：String, int。
示例：
{
  "merchant_1,20",
  "merchant_2,10"
}

计算规则
你需要按顺序遍历 transaction_list 中的每一笔交易，并根据以下三条规则，依次更新该交易对应商户的风险评分：

高额交易乘数规则：
如果当前交易的 交易金额 大于其对应规则中的 最低交易金额，则将该商户当前的风险评分乘以对应规则的 乘法因子。

高频客户累加规则：
对于同一个客户和同一个商户的组合，当交易次数超过3次时（即从第4笔交易开始），在处理这笔以及后续每一笔交易时，
都将该商户的风险评分加上该笔交易对应规则的 加法数值。

小时内高频交易规则：
对于同一个客户和同一个商户在同一个小时内的交易，当交易次数达到第3次或更多时（即从第3笔交易开始），根据交易时间进行如下操作：
如果交易时间在 12:00-17:00 之间（包含边界），则将商户的风险评分加上该笔交易对应规则的 罚金。
如果交易时间在 9:00-11:00 或 18:00-22:00 之间（包含边界），则将商户的风险评分减去该笔交易对应规则的 罚金。
在其他时间段，风险评分不变。

输出格式
一个 List<String>，其中每个字符串包含商户ID和其最终计算出的风险评分。

格式："商户ID,最终风险评分"。
"""
from collections import defaultdict

def solve(transaction_list, rules_list, merchants_list):
    n = len(transaction_list)

    rates = {}
    for item in merchants_list:
        merchant, r = item.split(",")
        rates[merchant] = int(r)
    
    rule2dict = defaultdict(int)
    rule3dict = defaultdict(lambda: [0] * 24)

    trs = []
    rules = []
    
    for i in range(n):
        trans, rule = transaction_list[i], rules_list[i]
        t1 = trans.split(",")
        m, amount, c, time = t1[0], int(t1[1]), t1[2], int(t1[3])
        trs.append((m, amount, c, time))
        r1 = rule.split(",")
        lowest_amount, multi, plus, fine = int(r1[0]), int(r1[1]), int(r1[2]), int(r1[3])
        rules.append((lowest_amount, multi, plus, fine))

    ## rule1
    for i in range(n):
        m, amount, c, time = trs[i]
        lowest_amount, multi, plus, fine = rules[i]

        if amount > lowest_amount:
            rates[m] *= multi

    ## rule2:
    first_two = defaultdict(list)

    for i in range(n):
        m, amount, c, time = trs[i]
        lowest_amount, multi, plus, fine = rules[i]

        rule2dict[(m, c)] += 1
        cnt = rule2dict[(m, c)]
        
        if cnt <= 2:
            first_two[(m, c)].append(plus)
        elif cnt == 3:
            rates[m] += sum(first_two[(m, c)])
            rates[m] += plus
        else:
            rates[m] += plus

    ## rule3
    fine_history = defaultdict(lambda: defaultdict(list))
    
    for i in range(n):
        m, amount, c, time = trs[i]
        lowest_amount, multi, plus, fine = rules[i]

        rule3dict[(m, c)][time] += 1
        fine_history[(m, c)][time].append(fine)
        
        cnt3 = rule3dict[(m, c)][time]  # 使用自己的计数变量

        if cnt3 < 3:
            continue

        if cnt3 == 3:
            total_pen = sum(fine_history[(m, c)][time][:3])
        else:
            total_pen = fine

        if 12 <= time <= 17:
            rates[m] += total_pen
        elif (9 <= time <= 11) or (18 <= time <= 21):
            rates[m] -= total_pen

    return rates


    
# transaction_list = ["merchant_1,1400,customer_1,10","merchant_1,1000,customer_1,10","merchant_2,2000,customer_1,15","merchant_2,500,customer_2,22"]
# rules_list = ["1000,2,9,15","2000,3,5,9","500,3,8,19","1600,2,17,9"]
# merchants_list =["merchant_1,20","merchant_2,10"]
# print(solve(transaction_list,rules_list,merchants_list))


transaction_list = [
"merchant1,1200,customer1,10",
"merchant1,500,customer1,10",
"merchant2,2400,customer1,15",
"merchant1,800,customer1,16",
"merchant1,1000,customer2,17",
"merchant1,1400,customer1,10",
]

rules_list = [
"1000,2,8,15",
"1400,5,3,19",
"2300,3,17,3",
"1800,2,9,6",
"1000,4,8,2",
"1200,3,11,7",
]

merchants_list = [
"merchant1,10",
"merchant2,20"
]

print(solve(transaction_list,rules_list,merchants_list))
