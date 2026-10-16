"""Provider state wording shared by Markdown and JSON presenters."""


PROVIDER_STATE_ZH = {"idle": "空跑（exit0 无报告）", "canceled": "已取消",
                     "failed": "已失败"}


def format_providers(providers):
    """Render structured provider records without parsing diagnostic text."""
    return "、".join("%s（%s）" % (pid, PROVIDER_STATE_ZH.get(state, state))
                    for pid, state in providers)
